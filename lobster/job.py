class JobProvider:
    def __init__(self):
        pass

    def obtain(self):
        raise NotImplementedError

    def release(self, id, return_code):
        raise NotImplementedError

    def done(self):
        raise NotImplementedError

class SimpleJobProvider:
    def __init__(self, cmd, num):
        self.__cmd = cmd
        self.__max = num
        self.__done = 0
        self.__running = 0
        self.__id = 0

    def obtain(self):
        if self.__done + self.__running < self.__max:
            self.__running += 1
            self.__id += 1
            print "Creating ", self.__id
            return (str(self.__id), self.__cmd, [], [])
        return None

    def release(self, id, return_code, output):
        print "Received", id, return_code, self.__done + 1, '/', self.__max
        self.__running -= 1
        if return_code == 0:
            self.__done += 1

    def done(self):
        return self.__done == self.__max
