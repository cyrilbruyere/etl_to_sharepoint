import pandas as pd
from config import wms2
from config import sharepoint
from shareplum import Site
from shareplum .site import Version
from io import StringIO

# Définition de la requête SQL
query = """
(select  distinct trunc(it.dstamp) AS DATES,
        it.supplier_id,
        ad_1.name AS SUPPLIER,
        it.customer_id,
        ad_2.name AS CUSTOMER,
        it.sku_id,
        it.reference_id,
        sk.description,
        vsp.standard_cost,
        it.update_qty,
        it.v_unit_of_measure,
        it.v_sales_multiple,
        it.container_id
from dcsdba.inventory_transaction it
left join dcsdba.sku sk on sk.sku_id = it.sku_id
left join dcsdba.v_sku_properties vsp on vsp.sku_id = it.sku_id
left join dcsdba.address ad_1 on ad_1.address_id = it.supplier_id
left join dcsdba.address ad_2 on ad_2.address_id = it.customer_id
where it.code = 'Shipment'
    and it.site_id = 'LDC'
    and vsp.site_id = 'LDC'
    and it.client_id = 'VOLVO'
    and sk.client_id = 'VOLVO'
    and vsp.client_id = 'VOLVO'
    and ad_1.client_id = 'VOLVO'
    and ad_2.client_id = 'VOLVO'
    and it.WORK_GROUP like '800%'
    and it.CONSIGNMENT = '-SHP-DCLYON')
--UNION
--(select  distinct trunc(ita.dstamp) AS DATES,
--        ita.supplier_id,
--        ad_1.name AS SUPPLIER,
--        ita.customer_id,
--        ad_2.name AS CUSTOMER,
--        ita.sku_id,
--        ita.reference_id,
--        sk.description,
--        vsp.standard_cost,
--        ita.update_qty,
--        ita.v_unit_of_measure,
--        ita.v_sales_multiple,
--        ita.container_id
--from dcsdba.inventory_transaction_archive ita
--left join dcsdba.sku sk on sk.sku_id = ita.sku_id
--left join dcsdba.v_sku_properties vsp on vsp.sku_id = ita.sku_id
--left join dcsdba.address ad_1 on ad_1.address_id = ita.supplier_id
--left join dcsdba.address ad_2 on ad_2.address_id = ita.customer_id
--where ita.code = 'Shipment'
--    and ita.site_id = 'LDC'
--    and vsp.site_id = 'LDC'
--    and ita.client_id = 'VOLVO'
--    and sk.client_id = 'VOLVO'
--    and vsp.client_id = 'VOLVO'
--    and ad_1.client_id = 'VOLVO'
--    and ad_2.client_id = 'VOLVO'
--    and ita.WORK_GROUP like '800%'
--    and ita.CONSIGNMENT = '-SHP-DCLYON')
"""

# Connection BD
conn_wms2 = wms2.connect()
# Execution requête
colonnes = ['DATES', 'SUPPLIER_ID', 'SUPPLIER', 'CUSTOMER_ID', 'CUSTOMER', 'SKU_ID', 'REFERENCE_ID', 'DESCRIPTION', 'SCOST', 'QTY', 'UNITES', 'SALES_QTY', 'CONTAINER_ID']
cursor = conn_wms2.cursor()
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns = colonnes)
conn_wms2.commit()
conn_wms2.close()

textstream = StringIO()
df.to_csv(textstream, index = False, sep = ';')
textstream.seek(0)
file_content = textstream.read()

sp_site = 'https://volvogroup.sharepoint.com/sites/unit-packaging-dc-lyon/'
sp_folder = 'Shared Documents/8-Outil PACK/2_DATA_OUT'

conn_sp = sharepoint.connect()

site = Site(sp_site, version = Version.v365, authcookie = conn_sp)

folder = site.Folder(sp_folder)

folder.upload_file(file_content, 'ppack_out.csv')