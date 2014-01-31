import sys
import os
import time

def update_indexes(directory):
    for root, dirs, files in os.walk(directory):
        current_dirs = dirs + ['..']
        current_dirs.sort()
        dir_names = ['<b>%s/</b>' % i for i in current_dirs]
        non_image_items = [file for file in files if 'pdf' not in file and 'png' not in file]
#        non_image_items.sort()
        non_image_item_paths = [os.path.join(root, i) for i in non_image_items]
        non_image_item_names = dir_names + non_image_items
        non_image_item_locations = current_dirs + non_image_items
        non_image_mod_times = ['' for i in current_dirs] + [time.ctime(os.path.getmtime(i)) for i in non_image_item_paths]
        png_images = [f for f in files if 'png' in f]
        pdf_images = [f.replace('.png', '.pdf') for f in png_images]
        png_images.sort()
        pdf_images.sort()
        snippet = '<tr><td><a href={location}>{name}</a></td><td>{mod_time}</td></tr>'
        files_snippet = '\n'.join([snippet.format(location=location, name=name, mod_time=mod_time) for (location, name, mod_time) in zip(non_image_item_locations, non_image_item_names, non_image_mod_times)])
        snippet = '<div class="pic photo-link smoothbox" id="{png}"><a href="{pdf}" rel="gallery"><img src="{png}" class="pic"/></a></div>'
        image_snippet = '\n'.join([snippet.format(pdf=pdf, png=png) for (pdf, png) in zip(pdf_images, png_images)])
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'index.html'), 'r') as f:
            index = f.read()
        with open(os.path.join(root, 'index.html'), 'w') as f:
            segment = index[index.find("<body>"):] #Have to do this because all of the javascript looks like python formatting statements
            modified_segment = segment.format(files=files_snippet, images=image_snippet)
            index = index.replace(segment, modified_segment)
            f.write(index)
