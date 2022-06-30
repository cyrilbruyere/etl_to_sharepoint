import pandas as pd
from config import wms2
from config import sharepoint
from shareplum import Site
from shareplum .site import Version
from io import StringIO

# Définition de la requête SQL flux E3/H9 (gares)
e3h9 = """
(select (extract(year from it.dstamp))*100-200000+(extract(month from it.dstamp)) as YYMM,
        it.code,
        it.sku_id,
        sum(it.update_qty) as UPDATE_QTY,
        it.from_loc_id,
        vsp.gate_id
from dcsdba.inventory_transaction it
left join dcsdba.v_sku_properties vsp on it.sku_id = vsp.sku_id
where it.code = 'Putaway'
and it.site_id = 'LDC'
and vsp.gate_id in('E3','H9')
and it.from_loc_id in('TR-EMP-VX1','TR-EMP-VX2','TR-EMP-G05','TR-EMP-GE1','TR-PRO-VX1')
and it.dstamp >= to_date('01/01/2022', 'DD/MM/YYYY')
group by ((extract(year from it.dstamp))*100-200000+(extract(month from it.dstamp)), it.code, it.sku_id, it.from_loc_id, vsp.gate_id)
)
UNION
(select (extract(year from ita.dstamp))*100-200000+(extract(month from ita.dstamp)) as YYMM,
        ita.code,
        ita.sku_id,
        sum(ita.update_qty) as UPDATE_QTY,
        ita.from_loc_id,
        vsp.gate_id
from dcsdba.inventory_transaction_archive ita
left join dcsdba.v_sku_properties vsp on ita.sku_id = vsp.sku_id
where ita.code = 'Putaway'
and ita.site_id = 'LDC'
and vsp.gate_id in('E3','H9')
and ita.from_loc_id in('TR-EMP-VX1','TR-EMP-VX2','TR-EMP-G05','TR-EMP-GE1','TR-PRO-VX1')
and ita.dstamp >= to_date('01/01/2022', 'DD/MM/YYYY')
group by ((extract(year from ita.dstamp))*100-200000+(extract(month from ita.dstamp)), ita.code, ita.sku_id, ita.from_loc_id, vsp.gate_id)
)
"""

# Définition de la requête SQL flux délestage
delestage = """
(select (extract(year from it.dstamp))*100-200000+(extract(month from it.dstamp)) as YYMM,
        it.code,
        it.sku_id,
        it.tag_id,
        sum(it.update_qty) as UPDATE_QTY,
        it.from_loc_id
from dcsdba.inventory_transaction it
where it.site_id = 'LDC'
and it.code = 'Putaway'
and it.dstamp >= to_date('01/01/2022', 'DD/MM/YYYY')
and it.from_loc_id in('TR-EMP-VX1','TR-EMP-VX2','TR-EMP-G05','TR-EMP-GE1','TR-PRO-VX1','KITSTT-ALG','KITSTT-APF')
--and it.from_loc_id in (select location_id
 --                      from dcsdba.location
 --                      where site_id = 'LDC'
 --                      and work_zone in ('RKIT','REMP-OUT','RPRO-OUT'))
group by ((extract(year from it.dstamp))*100-200000+(extract(month from it.dstamp)),it.code,it.sku_id,it.tag_id,it.from_loc_id)
)
UNION
(select (extract(year from ita.dstamp))*100-200000+(extract(month from ita.dstamp)) as YYMM,
        ita.code,
        ita.sku_id,
        ita.tag_id,
        sum(ita.update_qty) as UPDATE_QTY,
        ita.from_loc_id
from dcsdba.inventory_transaction_archive ita
where ita.site_id = 'LDC'
and ita.code = 'Putaway'
and ita.dstamp >= to_date('01/01/2022', 'DD/MM/YYYY')
and ita.from_loc_id in('TR-EMP-VX1','TR-EMP-VX2','TR-EMP-G05','TR-EMP-GE1','TR-PRO-VX1','KITSTT-ALG','KITSTT-APF')
--and ita.from_loc_id in (select location_id
--                        from dcsdba.location
--                        where site_id = 'LDC'
--                        and work_zone in ('RKIT','REMP-OUT','RPRO-OUT'))
group by ((extract(year from ita.dstamp))*100-200000+(extract(month from ita.dstamp)),ita.code,ita.sku_id,ita.tag_id,ita.from_loc_id)
)
"""

# Définition de la requête SQL flux cartons
cartons = """
select  (extract(year from dstamp))*100-200000+(extract(month from dstamp)) as YYMM,
        trunc(dstamp) as DATES,
        v_container_type as TYPES,
        v_infoship_status as STATUS,
        count(*) as QTY
from dcsdba.order_container
where v_site_id = 'LDC'
and dstamp >= to_date('01/01/2022', 'DD/MM/YYYY')
and v_container_type in ('7133', '7362', '7363', '7364', '7366', '7367', '7368')
group by ((extract(year from dstamp))*100-200000+(extract(month from dstamp)),trunc(dstamp),v_container_type,v_infoship_status)
"""

# Connection BD
conn_wms2 = wms2.connect()
cursor = conn_wms2.cursor()
# Préparation requêtes
colonnes_e3h9 = ['YYMM', 'CODE', 'SKU_ID', 'UPDATE_QTY', 'FROM_LOC_ID', 'GATE_ID']
colonnes_delestage = ['YYMM', 'CODE', 'SKU_ID', 'TAG_ID', 'UPDATE_QTY', 'FROM_LOC_ID']
colonnes_cartons = ['YYMM', 'DATES', 'TYPES', 'STATUS', 'QTY']
# Exécution requêtes
# cursor.execute(e3h9)
# e3h9 = pd.DataFrame(cursor.fetchall(), columns = colonnes_e3h9)
#
cursor.execute(delestage)
delestage = pd.DataFrame(cursor.fetchall(), columns = colonnes_delestage)
#
cursor.execute(cartons)
cartons = pd.DataFrame(cursor.fetchall(), columns = colonnes_cartons)
# Fermeture connection
conn_wms2.commit()
conn_wms2.close()

# Connection Sharepoint
sp_site = 'https://volvogroup.sharepoint.com/sites/unit-packaging-dc-lyon/'
sp_folder = 'Shared Documents/8-Outil PACK/1_DATA_IN'
conn_sp = sharepoint.connect()
site = Site(sp_site, version = Version.v365, authcookie = conn_sp)
folder = site.Folder(sp_folder)
# Chargemetnt csv sur sharepoint
# textstream_e3h9 = StringIO()
# e3h9.to_csv(textstream_e3h9, index = False, sep = ';')
# textstream_e3h9.seek(0)
# file_content_e3h9 = textstream_e3h9.read()
# folder.upload_file(file_content_e3h9, 'e3h9.csv')
#
textstream_delestage = StringIO()
delestage.to_csv(textstream_delestage, index = False, sep = ';')
textstream_delestage.seek(0)
file_content_delestage = textstream_delestage.read()
folder.upload_file(file_content_delestage, 'delestage.csv')
#
textstream_cartons = StringIO()
cartons.to_csv(textstream_cartons, index = False, sep = ';')
textstream_cartons.seek(0)
file_content_cartons = textstream_cartons.read()
folder.upload_file(file_content_cartons, 'cartons.csv')