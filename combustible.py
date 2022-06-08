import pandas as pd
from config import wms2
from config import sharepoint
from shareplum import Site
from shareplum .site import Version
from io import StringIO

# Définition de la requête SQL
query = """
SELECT i.SKU_ID,
    i.QTY_ON_HAND,
    i.LOCATION_ID,
    l.WORK_ZONE,
    s.EACH_WEIGHT
FROM DCSDBA.INVENTORY i
LEFT JOIN DCSDBA.SKU s ON i.SKU_ID = s.SKU_ID
LEFT JOIN DCSDBA.LOCATION l ON i.LOCATION_ID = l.LOCATION_ID
WHERE i.SITE_ID = 'LDC'
AND s.CLIENT_ID = 'VOLVO'
"""

# Connection BD
conn_wms2 = wms2.connect()
# Execution requête
colonnes = ['SKU_ID', 'QTY_ON_HAND', 'LOCATION_ID', 'WORK_ZONE', 'EACH_WEIGHT']
cursor = conn_wms2.cursor()
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns = colonnes)
conn_wms2.commit()
conn_wms2.close()

textstream = StringIO()
df.to_csv(textstream, index = False)
textstream.seek(0)
file_content = textstream.read()

sp_site = 'https://volvogroup.sharepoint.com/sites/unit-qlikview-update/'
sp_folder = 'Shared Documents/Qlikview - Data/Combustible'

conn_sp = sharepoint.connect()

site = Site(sp_site, version = Version.v365, authcookie = conn_sp)

folder = site.Folder(sp_folder)

folder.upload_file(file_content, 'combustible.csv')