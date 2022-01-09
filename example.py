from esstatistikliste import *
import json

it = XmlJsonBatchIterator("/media/orest/9A40A9F540A9D7F1/ESStatistikListeModtag/ESStatistikListeModtag.xml",1000)
for i,x in zip(range(10),it):
    print (i,len(x), len(json.loads(x)))
