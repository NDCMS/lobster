from collections import defaultdict

import logging

logger = logging.getLogger('lobster.algo')


class Algo(object):

    """A task creation algorithm

    Attempts to be fair when creating tasks by making sure that tasks are
    created evenly for every category and every workflow in each category
    based on the remaining work per workflow and cores used.

    Parameters
    ----------
        config : Configuration
            The Lobster configuration to use.
    """

    def __init__(self, config):
        self.__config = config

    def run(self, total_cores, queued, remaining):
        """Run the task creation algorithm.

        Steps
        -----
        1. Calculate remaining workload, weighed by cores, per category
        2. Determine how many cores need to be filled
        3. Go through workflows:
           1. Determine the fraction of category workload versus the total
              workload
           2. Do the same for the workflow workload versus the category
              workload
           3. Use the first fraction to calculate how many tasks should be
              created for the category
           4. Adjust for mininum queued and maximum total task requirements
           5. Subtract already queued tasks
           6. Calculate how many tasks should be created for the current
              workflow based on the previously calculated fraction
           7. Adjust task size taper based on available tasks and needed
              tasks

        Parameters
        ----------
            total_cores : int
                The number of cores that `WorkQueue` currently is in
                control of.
            queued : dict
                A dictionary containing information about the queue on a
                per category basis.  Keys are category names, values are
                dictionaries with the keys `running` and `queued`, denoting
                how many category tasks fall into each bin.
            remaining : dict
                A dictionary with workflows as keys, and a tuple containing
                the following as value:

                * if all units for the workflow are available
                * how many units are left to process
                * how many tasks can still be created with the default size

        Returns
        -------
            data : list
                A list containing workflow label, how many tasks to create,
                and the task taper adjustment.
        """
        # Remaining workload
        workloads = defaultdict(int)
        for wflow, (complete, units, tasks) in remaining.items():
            if not complete and tasks < 1.:
                logger.debug("workflow {} has not enough units available to form new tasks".format(wflow.label))
                continue
            elif units == 0:
                continue
            ncores = wflow.category.cores or 1
            workloads[wflow.category.name] += ncores * tasks

        # How many cores we need to occupy: have at least 10% of the
        # available cores provisioned with waiting work
        fill_cores = total_cores + max(int(0.1 * total_cores), self.__config.advanced.payload)
        total_workload = sum(workloads.values())

        if total_workload == 0:
            return []

        # contains (workflow label, tasks, taper)
        data = []
        for wflow, (complete, units, tasks) in remaining.items():
            if not complete and tasks < 1. or units == 0:
                continue
            ncores = wflow.category.cores or 1
            category_fraction = workloads[wflow.category.name] / float(total_workload)
            workflow_fraction = ncores * tasks / float(workloads[wflow.category.name])

            needed_category_tasks = category_fraction * fill_cores / ncores

            if wflow.category.tasks_max:
                allowed = wflow.category.tasks_max - sum(queued[wflow.category.name].values())
                needed_category_tasks = min(allowed, needed_category_tasks)
            if wflow.category.tasks_min:
                required = wflow.category.tasks_min - queued[wflow.category.name]['queued']
                needed_category_tasks = max(required, needed_category_tasks)

            needed_category_tasks -= queued[wflow.category.name]['queued']
            needed_workflow_tasks = max(0, int(needed_category_tasks * workflow_fraction))

            if needed_category_tasks <= 0:
                continue

            taper = 1.
            if needed_workflow_tasks < tasks and complete:
                taper = min(1., tasks / float(needed_workflow_tasks))

            logger.debug(("creating tasks for {w.label} (category: {w.category.name}):\n" +
                          "\tcategory task limit: ({w.category.tasks_min}, {w.category.tasks_max})\n" +
                          "\tcategory tasks needed: {0}\n" +
                          "\tworkflow tasks needed: {1}\n" +
                          "\tworkflow tasks available: {2}\n" +
                          "\ttask taper: {3}").format(needed_category_tasks, needed_workflow_tasks, tasks, taper, w=wflow))

            data.append((wflow.label, needed_workflow_tasks, taper))

            # adjust accounting for next workflow
            queued[wflow.category.name]['queued'] += needed_workflow_tasks

        return data
