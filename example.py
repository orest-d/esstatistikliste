from esstatistikliste import *
import json

path = "/media/orest/9A40A9F540A9D7F1/ESStatistikListeModtag/"

def simple_json_batches(path):
    it = XmlJsonBatchIterator(path+"ESStatistikListeModtag.xml",1024)
    #for i,x in zip(range(10),it):
    for i,x in enumerate(it):
        print (i,len(x))
        with open(path+f"json/{i}.json","w") as f:
            f.write(x)

def json_batches_registered_only(path):
    it = XmlRegisteredJsonBatchIterator(path+"ESStatistikListeModtag.xml",1024)
    #for i,x in zip(range(10),it):
    for i,x in enumerate(it):
        print (i,len(x))
        with open(path+f"json_reg/{i}.json","w") as f:
            f.write(x)

def json_batches_registered_dict_of_lists(path):
    it = XmlRegisteredDictOfListsJsonBatchIterator(path+"ESStatistikListeModtag.xml",1024)
    #for i,x in zip(range(10),it):
    for i,x in enumerate(it):
        print (i,len(x))
        with open(path+f"reg_dol_json/{i}.json","w") as f:
            f.write(x)

json_batches_registered_only(path)