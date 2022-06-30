import pandas as pd
from config import wms2
from config import sharepoint
from shareplum import Site
from shareplum .site import Version
from io import StringIO

# Définition de la requête SQL
query = """
SELECT mt.container_id,
       'MT'                                     "Table",
       TO_CHAR (mt.key)                         "Key",
       mt.customer_id                           "Customer ID",
       oh.Order_id                              "DLX Order ID",
       ol.line_id                               "DLX Line ID",
       ol.user_def_type_1                       "CRM Order ID",
       ol.user_def_num_3                        "CRM Line ID",
       ol.v_dealer_ord_no                       "DEA Order ID",
       ol.v_dealer_ord_line_no                  "DEA Line ID",
       TRUNC (mt.dstamp)                        "Date",
       TO_CHAR (mt.dstamp, 'HH24:MI:SS')        "Hour",
       TRUNC (oh.ship_by_date)                  "Ship by date",
       mt.sku_id                                "Sku ID",
       ol.qty_ordered                           "Qty Ordered",
       mt.qty_to_move                           "Qty to move",
       sp.sales_multiple                        "Sales Multiple",
       NULL                                     "Load List",
       oh.user_def_chk_6                        "Not Combined",
       mt.status                                "Status",
       mt.work_zone                             "Work Zone",
       mt.pallet_id                             "Pallet ID",
       mt.work_group                            "Work Group",
       mt.consignment                           "Consignment",
       mt.from_loc_id                           "From location",
       mt.to_loc_id                             "To location",
       oh.order_type                            "Order Type",
       NULL                                     "Container Type",
       NULL                                     "Pallet weight",
       NULL                                     "Pallet volume",
       NULL                                     "Carrier ID",
       NULL                                     "Trailer ID",
       NULL                                     "Service Level",
       NULL                                     "Dispatch Method",
NULL                                             "Width" ,
       NULL                                     "Height",
       NULL                                     "Depth",
       mt.qty_to_move * ol.v_export_line_value  "Value",
       ol.user_def_type_3                       "Currency",
       hazmat_id                                "Hazmat ID",
       NULL                                     "Dock Door",
       oh.ship_dock                             "Shipdock ID",
       mt.qty_to_move * s.each_weight           "Net weight",
       s.each_weight                            "Each weight",
       s.each_volume                            "Each volume",
       NULL                                     "Tare",
       final_loc_id,
       (SELECT DISTINCT work_zone
          FROM dcsdba.location
         WHERE site_id = 'LDC' AND location_id = mt.final_loc_id) "Quai",
       TO_CHAR (SYSDATE, 'DD/MM/YYYY HH24:MI:SS')       "Date/Hour"
  FROM dcsdba.move_task         mt,
       dcsdba.order_header      oh,
       dcsdba.order_line        ol,
       dcsdba.sku               s,
       dcsdba.v_sku_properties  sp
WHERE     mt.site_id = 'LDC'
       AND mt.client_id = 'VOLVO'
       AND mt.container_id IS NULL
       AND final_loc_id IN
               (SELECT DISTINCT location_id
                  FROM dcsdba.location
                 WHERE     site_id = 'LDC'
                       AND loc_type = 'ShipDock'
                       AND (work_zone = 'QUAI-G6' or work_zone = 'SOFLOG'))
       AND oh.ship_dock NOT IN ('AEC-EXPKIT', 'LDC--SCRAP', 'LDC-RETURN')
       AND mt.client_id = oh.client_id
       AND mt.task_id = oh.order_id
       AND ol.order_id = mt.task_id
       AND ol.line_id = mt.line_id
       AND mt.sku_id = s.sku_id
       AND mt.client_id = s.client_id
       AND sp.site_id = mt.site_id
       AND sp.client_id = mt.client_id
       AND sp.sku_id = mt.sku_id
UNION
-- PARTIE 1bis - MOVE TAKS  avec Container
SELECT mt.container_id,
       'MC'                                     "Table",
       TO_CHAR (mt.key)                         "Key",
       mt.customer_id                           "Customer ID",
       oh.Order_id                              "DLX Order ID", 
       ol.line_id                               "DLX Line ID",
       ol.user_def_type_1                       "CRM Order ID",
       ol.user_def_num_3                        "CRM Line ID",
       v_dealer_ord_no                          "DEA Order ID",
       v_dealer_ord_line_no                     "DEA Line ID",
       TRUNC (mt.dstamp)                        "Date",
       TO_CHAR (mt.dstamp, 'HH24:MI:SS')        "Hour",
       TRUNC (oh.ship_by_date)                  "Ship by date",
       mt.sku_id                                "Sku ID",
       ol.qty_ordered                           "Qty Ordered",
       mt.qty_to_move                           "Qty to move",
       sp.sales_multiple                        "Sales Multiple",
       ll.list_id                               "Load List ID",
       oh.user_def_chk_6                        "Not Combined",
       mt.status                                "Status",
       mt.work_zone                             "Work Zone",
       mt.pallet_id                             "Pallet ID",
       mt.work_group                            "Work Group",
       mt.consignment                           "Consignment",
       mt.from_loc_id                           "From location",
       mt.to_loc_id                             "To location",
       order_type                               "Order Type",
       oc.v_container_type                      "Container Type",
       oc.pallet_weight                         "Pallet Weight",
       oc.pallet_volume / 1000                  "Pallet volume",
       oc.v_carrier_id                          "Carrier ID",
       oc.v_trailer_id                          "Trailer ID",
       oc.v_service_level                       "Service Level",
       oc.v_dispatch_method                     "Dispatch Method",
       oc.v_width                               "Width",
       oc.v_height                              "Height",
       oc.v_depth                               "Depth",
       mt.qty_to_move * ol.v_export_line_value  "Value",
       ol.user_def_type_3                       "Currency",
       hazmat_id                                "Hazmat ID",
       ct.DOCK_DOOR_id                          "Dock Door",
       oh.ship_dock                             "Shipdock ID" ,
       mt.qty_to_move * s.each_weight           "Net weight",
       s.each_weight                            "Each weight",
       s.each_volume                            "Each volume",
       pc.WEIGHT                                "Tare",
       final_loc_id,
       (SELECT DISTINCT work_zone
          FROM dcsdba.location
         WHERE site_id = 'LDC' AND location_id = mt.final_loc_id) "Quai",
       TO_CHAR (SYSDATE, 'DD/MM/YYYY HH24:MI:SS') "Date/Hour"
  FROM dcsdba.move_task            mt,
       dcsdba.order_header         oh,
       dcsdba.order_container      oc,
       dcsdba.order_line           ol,
       dcsdba.sku                  s,
       dcsdba.v_load_list_items    ll,
       dcsdba.consignment_trailer  ct,
       dcsdba.pallet_config        pc,
       dcsdba.v_sku_properties     sp
WHERE     mt.site_id = 'LDC'
       AND mt.client_id = 'VOLVO'
       AND mt.container_id IS NOT NULL
       AND final_loc_id IN
               (SELECT DISTINCT location_id
                  FROM dcsdba.location
                 WHERE     site_id = 'LDC'
                       AND loc_type = 'ShipDock'
                       AND (work_zone = 'QUAI-G6' or work_zone = 'SOFLOG'))
       AND oh.ship_dock NOT IN ('AEC-EXPKIT', 'LDC--SCRAP', 'LDC-RETURN')
       AND mt.client_id = oh.client_id
       AND mt.task_id = oh.order_id
       AND oc.ORDER_ID(+) = mt.task_id
       AND oc.CLIENT_ID(+) = mt.client_id
       AND oc.CONTAINER_ID(+) = mt.CONTAINER_ID
       AND oc.PALLET_ID(+) = mt.PALLET_ID
       AND oc.V_SITE_ID(+) = mt.site_id
       AND ol.order_id = mt.task_id
       AND ol.line_id = mt.line_id
       AND mt.sku_id = s.sku_id
       AND mt.client_id = s.client_id
       AND mt.pallet_id = ll.pallet_id(+)
       AND ct.site_id = mt.site_id
       AND ct.consignment = mt.consignment
       AND ct.trailer_id = oc.v_trailer_id
       AND oc.v_container_type = pc.config_id(+)
       AND sp.site_id = mt.site_id
       AND sp.client_id = mt.client_id
       AND sp.sku_id = mt.sku_id
"""

# Connection BD
conn_wms2 = wms2.connect()
# Execution requête
colonnes = ['CONTAINER_ID', 'Table', 'Key', 'Customer ID', 'DLX Order ID', 'DLX Line ID', 'CRM Order ID', 'CRM Line ID', 'DEA Order ID', 'DEA Line ID', 'Date', 'Hour',
                'Ship by date', 'Sku ID', 'Qty Ordered', 'Qty to move', 'Sales Multiple', 'Load List ID', 'Not Combined', 'Status', 'Work Zone', 'Pallet ID', 'Work Group',
                'Consignment', 'From location', 'To location', 'Order Type', 'Container Type', 'Pallet weight', 'Pallet volume', 'Carrier ID', 'Trailer ID', 'Service Level',
                'Dispatch Method', 'Width', 'Height', 'Depth', 'Value', 'Currency', 'Hazmat ID', 'Dock Door', 'Shipdock ID', 'Net weight', 'Each weight', 'Each volume', 'Tare',
                'FINAL_LOC_ID', 'Quai', 'Date/hour', ]
cursor = conn_wms2.cursor()
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns = colonnes)
conn_wms2.commit()
conn_wms2.close()

textstream = StringIO()
df.to_csv(textstream, index = False, sep = ';')
textstream.seek(0)
file_content = textstream.read()

sp_site = 'https://volvogroup.sharepoint.com/sites/unit-wms3-french-team/'
sp_folder = 'Shared Documents/Export Data'

conn_sp = sharepoint.connect()

site = Site(sp_site, version = Version.v365, authcookie = conn_sp)

folder = site.Folder(sp_folder)

folder.upload_file(file_content, 'export-loadlist.csv')