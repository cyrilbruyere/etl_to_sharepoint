import pandas as pd
from config import wms2
from config import sharepoint
from shareplum import Site
from shareplum .site import Version
from io import StringIO

# Définition de la requête SQL
query = """
(select distinct
        it.sku_id,
        vpmc.prepack_code,
        vpm.material_sku_id,
        it.condition_id,
        trunc(it.dstamp) as Dates,
        (extract(year from it.dstamp))*100+(extract(month from it.dstamp)) as YYYYMM,
        it.update_qty/vsp.sales_multiple as UV,
        it.tag_id,
        vsp.gate_id as Gare,
        it.from_loc_id,
        it.supplier_id,
        ad.name
from dcsdba.inventory_transaction it
left join dcsdba.v_sku_properties vsp on vsp.sku_id = it.sku_id
left join dcsdba.v_prepack_mat_codes vpmc on vpmc.sku_id = it.sku_id
left join dcsdba.v_prepack_materials vpm on vpm.sku_id = it.sku_id
left join dcsdba.address ad on ad.ADDRESS_ID = it.supplier_id
where it.condition_id in ('PPACK','KIT')
and it.code='Receipt'
and it.site_id = 'LDC'
and it.client_id = 'VOLVO'
and vsp.client_id='VOLVO'
and vsp.gate_id is not null
and vpm.sequence_num = 1
and vpm.site_id = 'LDC'
and vpm.client_id = 'VOLVO'
and vpmc.site_id = 'LDC'
and vpmc.client_id = 'VOLVO'
)
UNION
(select distinct
        ita.sku_id,
        vpmc.prepack_code,
        vpm.material_sku_id,
        ita.condition_id,
        trunc(ita.dstamp) as Dates,
        (extract(year from ita.dstamp))*100+(extract(month from ita.dstamp)) as YYYYMM,
        ita.update_qty/vsp.sales_multiple as UV,
        ita.tag_id,
        vsp.gate_id as Gare,
        ita.from_loc_id,
        ita.supplier_id,
        ad.name
from dcsdba.inventory_transaction_archive ita
left join dcsdba.v_sku_properties vsp on vsp.sku_id = ita.sku_id
left join dcsdba.v_prepack_mat_codes vpmc on vpmc.sku_id = ita.sku_id
left join dcsdba.v_prepack_materials vpm on vpm.sku_id = ita.sku_id
left join dcsdba.address ad on ad.ADDRESS_ID = ita.supplier_id
where ita.condition_id in ('PPACK','KIT')
and ita.code='Receipt'
and ita.site_id = 'LDC'
and ita.client_id = 'VOLVO'
and vsp.client_id='VOLVO'
and vsp.gate_id is not null
and vpm.sequence_num = 1
and vpm.site_id = 'LDC'
and vpm.client_id = 'VOLVO'
and vpmc.site_id = 'LDC'
and vpmc.client_id = 'VOLVO'
)
"""

# Connection BD
conn_wms2 = wms2.connect()
# Execution requête
colonnes = ['SKU_ID', 'PREPACK_CODE', 'MATERIAL_SKU_ID', 'CONDITION_ID', 'Dates', 'YYYYMM', 'UV', 'TAG_ID', 'Gare', 'FROM_LOC_ID', 'SUPPLIER_ID', 'NAME']
cursor = conn_wms2.cursor()
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns = colonnes)
conn_wms2.commit()
conn_wms2.close()

textstream = StringIO()
df.to_csv(textstream, index = False)
textstream.seek(0)
file_content = textstream.read()

sp_site = 'https://volvogroup.sharepoint.com/sites/unit-packaging-dc-lyon/'
sp_folder = 'Shared Documents/Outil PACK/1_DATA_IN'

conn_sp = sharepoint.connect()

site = Site(sp_site, version = Version.v365, authcookie = conn_sp)

folder = site.Folder(sp_folder)

folder.upload_file(file_content, 'ppack_in.csv')