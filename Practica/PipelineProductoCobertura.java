package com.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.sql.Types;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineProductoCobertura {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineProductoCobertura.class);
   
    public static void main(String[] args) throws InterruptedException {
    	//---INICIO PIPELINE PRODUCTOS----------------------------------------------------   
    	    	  // Configuración del PipelineOptions
                DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
                String projectId = System.getenv("PROJECT_ID");
                if (projectId == null) projectId = "fid-cmch-pricing-dev";
                String tempLocation = System.getenv("TEMP_LOCATION");
                if (tempLocation == null) tempLocation = "";
                String region = System.getenv("REGION");
                if (region == null) region = "us-east4";
                String subnetwork = System.getenv("Subnetwork");
                if (subnetwork == null ) subnetwork = "";
                options.setSubnetwork(subnetwork);
                options.setProject(projectId);
                options.setRegion(region);
                options.setTempLocation(tempLocation);
                options.setRunner(DirectRunner.class);
                LOG.info("Pipeline configurado con Project ID: {}, Region: {}, Temp Location: {}", projectId, region, tempLocation);

                // Crear pipeline
                Pipeline pipeline = Pipeline.create(options);

                BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        		// Lista de tablas origen y sus respectivos snapshots
        		List<String> tables = Arrays.asList(
        				"fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_cial_fp",
        				"fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_tec_cial_fp",
        				"fid-cmms-datalake-dev.sise_fid_datastrreaming.tiso_producto_fp"
        		);

        		// Crear snapshots para cada tabla
        		for (String sourceTable : tables) {
        			String snapshotTable = sourceTable + "_snapshot";
        			String query = String.format(
        					"CREATE SNAPSHOT TABLE `%s` CLONE `%s` OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))",
        					snapshotTable, sourceTable
        			);
        			System.out.println("Creando snapshot para: " + sourceTable);
        			
        			// Configurar y ejecutar la consulta
        			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
        					.setUseLegacySql(false)
        					.build();
        			Job job = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        			job = job.waitFor();
        			if (job.isDone()) {
        				System.out.println("Snapshot creado: " + snapshotTable);
        			} else {
        				System.out.println("Error al crear snapshot: " + job.getStatus().getError());
        			}
        		}    

            
            // Leer tablas desde BigQuery
            PCollection<TableRow> tableRows = pipeline.apply("Leer Tabla 1", BigQueryIO.readTableRows()
                    .from("fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_cial_fp_snapshot"));
            PCollection<TableRow> renamedTableRows = tableRows.apply("Renombrar Campo", ParDo.of(new DoFn<TableRow, TableRow>() {
                @ProcessElement
                public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
                    // Crear una nueva fila
                    TableRow newRow = new TableRow();

                    // Iterar sobre los campos del TableRow original
                    for (Map.Entry<String, Object> entry : row.entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();

                        // Si el campo es "cod_producto", usar el nuevo nombre
                        if ("cod_producto".equals(key)) {
                            newRow.set("nuevo_cod_producto", value);
                        }else if("txt_desc".equals(key)) {
                        	newRow.set("nuevo_txt_desc", value);
                        } else {
                            // Mantener los demás campos igual
                            newRow.set(key, value);
                        }
                    }
                    // Emitir la nueva fila
                    out.output(newRow);
                }
            }));
            
                PCollection<TableRow> tableRows2 = pipeline.apply("Leer Tabla 2", BigQueryIO.readTableRows()
                    .from("fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_tec_cial_fp_snapshot"));

                PCollection<TableRow> tableRows3 = pipeline.apply("Leer Tabla 3", BigQueryIO.readTableRows()
                    .from("fid-cmms-datalake-dev.sise_fid_datastrreaming.tiso_producto_fp_snapshot"));
    	        // Convertir cada tabla en Key-Value pairs
    	        PCollection<KV<String, TableRow>> kvTable1 = renamedTableRows.apply(ParDo.of(new ExtractKeyFn("cod_ramo", "nuevo_cod_producto")));
    	        PCollection<KV<String, TableRow>> kvTable2 = tableRows2.apply(ParDo.of(new ExtractKeyFn("cod_ramo_com", "cod_producto_cial")));
    	        PCollection<KV<String, TableRow>> kvTable3 = tableRows3.apply(ParDo.of(new ExtractKeyFn("cod_ramo", "cod_producto")));

    	        // Agrupar por clave
    	        PCollection<KV<String, CoGbkResult>> joinedTables = KeyedPCollectionTuple
    	            .of(new TupleTag<>("table1"), kvTable1)
    	            .and(new TupleTag<>("table2"), kvTable2)
    	            .apply(CoGroupByKey.create());
     
    	        PCollection<TableRow> finalTable = joinedTables.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
    	            @ProcessElement
    	            public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<TableRow> out) {
    	                CoGbkResult result = element.getValue();
    	                Iterable<TableRow> table1Rows = result.getAll(new TupleTag<>("table1"));
    	                Iterable<TableRow> table2Rows = result.getAll(new TupleTag<>("table2"));
    	                for (TableRow row2 : table2Rows) {
    	                    for (TableRow row1 : table1Rows) {     	 
    	                        TableRow newRow = new TableRow();
    	                        newRow.set("cod_ramo", row2.get("cod_ramo"));
    	                        newRow.set("cod_producto_tec", (int) Double.parseDouble(row2.get("cod_producto_tec").toString()));
    	                   //   newRow.set("PlanTecnicoID", row3.get("cod_producto"));
    	                   //   newRow.set("PlanTecnico", row3.containsKey("txt_desc") ? row3.get("txt_desc") : "Sin Plan");
    	                        newRow.set("cod_ramo_com", row2.containsKey("cod_ramo_com") ? row2.get("cod_ramo_com") : 0);
    	                        newRow.set("nuevo_cod_producto", row1.get("nuevo_cod_producto"));
    	                        newRow.set("nuevo_txt_desc", row1.get("nuevo_txt_desc"));
    	                   //   newRow.set("ProductoFechaDesde", row3.get("fec_vig_desde"));
    	                   //   newRow.set("ProductoFechaHasta", row3.get("fec_vig_hasta"));
    	                   //   newRow.set("EstadoHabilitado", 0);
    	                   //   newRow.set("SucursalRamoComercialID", row3.get("cod_suc")); 

    	                        out.output(newRow);  // Emitir una fila por cada coincidencia de table1
    	                    	}} }
    	            }
));
    	        
    	    PCollection<KV<String, TableRow>> kvTable4 = finalTable.apply(ParDo.of(new ExtractKeyFn("cod_ramo", "cod_producto_tec")));

	        PCollection<KV<String, CoGbkResult>> joinedTables2 = KeyedPCollectionTuple
    	            .of(new TupleTag<>("table3"), kvTable3)
    	            .and(new TupleTag<>("table4"), kvTable4)
    	            .apply(CoGroupByKey.create());
    	    
	        
	        PCollection<TableRow> finalTable2 = joinedTables2.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
	            @ProcessElement
	            public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<TableRow> out) {
	                CoGbkResult result = element.getValue();
	                Iterable<TableRow> table3Rows = result.getAll(new TupleTag<>("table3"));
	                Iterable<TableRow> table4Rows = result.getAll(new TupleTag<>("table4"));
	                int ProductoID = 0;
	                for (TableRow row3 : table3Rows) {
	                    for (TableRow row4 : table4Rows) {
	                    	 
	                        TableRow newRow = new TableRow();
	                        newRow.set("RamoPlanId", row3.get("cod_ramo"));
	                        newRow.set("PlanTecnicoID", row3.get("cod_producto"));
	                        newRow.set("PlanTecnico", row3.containsKey("txt_desc") ? row3.get("txt_desc") : "Sin Plan");
	                        newRow.set("RamoComercialID", row4.containsKey("cod_ramo_com") ? row4.get("cod_ramo_com") : 0);
	                        newRow.set("ProductoComercialID", row4.get("nuevo_cod_producto"));
	                        newRow.set("ProductoComercial", row4.get("nuevo_txt_desc"));
	       //               newRow.set("ProductoFechaDesde", row3.get("fec_vig_desde"));
	       //               newRow.set("ProductoFechaHasta", row3.get("fec_vig_hasta"));
	                        newRow.set("EstadoHabilitado", 0);
	                        newRow.set("SucursalRamoComercialID", row3.get("cod_suc")); 
	                        newRow.set("ProductoID", ProductoID += 1); 
	                        

	                        out.output(newRow);
	                    	}} }
	            }
));    
            String cloudSqlUrl = "";
            String user = "";
            String password = "";
	        //Subir Tabla PRODUCTOS a la cloud
	        // credencialesDB

         // Insert datos MySQL
            finalTable2.apply("EscribirEnMySQL",
        	            JdbcIO.<TableRow>write()
        	            .withDataSourceConfiguration(
        	            JdbcIO.DataSourceConfiguration.create(
        	                "com.mysql.cj.jdbc.Driver", cloudSqlUrl)
        	                .withUsername(user)
        	                .withPassword(password))
.withStatement("INSERT INTO Producto (RamoPlanId, PlanTecnicoID, PlanTecnico, TipoProductoId, CategoriaId, RamoComercialID, ProductoComercialID, ProductoComercial, SucursalRamoComercialID, EstadoHabilitado, ProductoTecnicoId, Moneda) \r\n"
+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\r\n")
.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
        	            @Override
        	            public void setParameters(TableRow row, PreparedStatement query) throws Exception {
    	                Object RamoPlanID = row.get("RamoPlanId");
    	                Object PlanTecnicoID = row.get("PlanTecnicoID");
    	                Object PlanTecnico = row.get("PlanTecnico");
    	                Object ProductoComercialID = row.get("ProductoComercialID");
    	                Object ProductoComercial = row.get("ProductoComercial");
    	                Object RamoComercialID = row.get("RamoComercialID");
    	                Object EstadoHabilitado = row.get("EstadoHabilitado");
    	                Object SucursalRamoComercialID = row.get("SucursalRamoComercialID");
    	                Object TipoProductoId = null;
    	                Object CategoriaId = null;
    	                Object ProductoTecnicoId = null;
    	                Object Moneda = null;          
    	                
         	            query.setInt(1, RamoPlanID != null ? (int) Double.parseDouble(RamoPlanID.toString()) : 0);
        	            query.setInt(2, PlanTecnicoID != null ? (int) Double.parseDouble(PlanTecnicoID.toString()) : 0);
    	                query.setString(3, PlanTecnico != null ? PlanTecnico.toString() : null);
    	                query.setInt(4, TipoProductoId != null ? (int) Double.parseDouble(TipoProductoId.toString()) : 0);
    	                query.setInt(5, CategoriaId != null ? (int) Double.parseDouble(CategoriaId.toString()) : 0);
    	                query.setInt(6, RamoComercialID != null ? (int) Double.parseDouble(RamoComercialID.toString()) : 0);
    	                query.setInt(7, ProductoComercialID != null ? (int) Double.parseDouble(ProductoComercialID.toString()) : 0);
    	                query.setString(8, ProductoComercial != null ? ProductoComercial.toString() : null);
    	                query.setInt(9, SucursalRamoComercialID != null ? (int) Double.parseDouble(SucursalRamoComercialID.toString()) : 0);
    	                query.setInt(10, EstadoHabilitado != null ? (int) EstadoHabilitado : 0);
    	                query.setInt(11, ProductoTecnicoId != null ? (int) Double.parseDouble(ProductoTecnicoId.toString()) : 0);
    	                query.setString(12, Moneda != null ? Moneda.toString() : null);
    	                
        	            }
        	        }));      
        	        
        pipeline.run().waitUntilFinish();
       //Obtener Tabla PRODUCTOS desde la cloud
        Pipeline p = Pipeline.create(options);

    	PCollection<TableRow> productos = p.apply("LeerProductos",
                JdbcIO.<TableRow>read()
                    .withDataSourceConfiguration(
                        DataSourceConfiguration.create(
                            "com.mysql.cj.jdbc.Driver", cloudSqlUrl)
                            .withUsername(user)
                            .withPassword(password))
                    .withQuery("SELECT * FROM Producto")
                    .withRowMapper(new JdbcIO.RowMapper<TableRow>() {
                        @Override
                        public TableRow mapRow(ResultSet resultSet) throws Exception {
                            // Crear un nuevo TableRow y mapear los resultados de la consulta
                            TableRow row = new TableRow();
                            row.set("RamoPlanID", resultSet.getInt("RamoPlanID"));
                            row.set("PlanTecnicoID", resultSet.getInt("PlanTecnicoId"));
                            row.set("PlanTecnico", resultSet.getString("PlanTecnico"));
                            row.set("TipoProductoId", resultSet.getInt("TipoProductoId"));
                            row.set("CategoriaId", resultSet.getInt("CategoriaId"));
                            row.set("RamoComercialID", resultSet.getInt("RamoComercialId"));
                            row.set("ProductoComercialID", resultSet.getInt("ProductoComercialId"));
                            row.set("ProductoComercial", resultSet.getString("ProductoComercial"));
                            row.set("SucursalRamoComercialID", resultSet.getInt("SucursalRamoComercialId"));
                            row.set("EstadoHabilitado", resultSet.getInt("EstadoHabilitado"));
                            row.set("ProductoId", resultSet.getInt("ProductoId"));
                            row.set("ProductoTecnicoId", resultSet.getInt("ProductoTecnicoId"));
                            row.set("Moneda", resultSet.getString("Moneda"));
                            return row;
                        }
                    })); 
        
    	        	//---FIN PIPELINE DE PRODUCTOS----------------------------------------------------        
    	        	        System.out.println("----------------------------------------------------------------------------------");
    	        	//---INICIO PIPELINE COBERTURAS----------------------------------------------------    
    	        	        
    	        	        
    	        	        BigQuery bigqueryCobertura = BigQueryOptions.getDefaultInstance().getService();

    	            		// Lista de tablas origen y sus respectivos snapshots
    	            		List<String> tablesCobertura = Arrays.asList(
    	            				"fid-cmms-datalake-dev.sise_fid_datastrreaming.ttarifa_producto_fp",
    	            				"fid-cmms-datalake-dev.sise_fid_datastrreaming.ttarifa_fp"
    	            		);

    	            		// Crear snapshots para cada tabla
    	            		for (String sourceTable : tablesCobertura) {
    	            			String snapshotTable = sourceTable + "_snapshot";
    	            			String query = String.format(
    	            					"CREATE SNAPSHOT TABLE `%s` CLONE `%s` OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))",
    	            					snapshotTable, sourceTable
    	            			);
    	            			System.out.println("Creando snapshot para: " + sourceTable);
    	            			
    	            			// Configurar y ejecutar la consulta
    	            			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
    	            					.setUseLegacySql(false)
    	            					.build();
    	            			Job job = bigqueryCobertura.create(JobInfo.newBuilder(queryConfig).build());
    	            			job = job.waitFor();
    	            			if (job.isDone()) {
    	            				System.out.println("Snapshot creado: " + snapshotTable);
    	            			} else {
    	            				System.out.println("Error al crear snapshot: " + job.getStatus().getError());
    	            			}
    	            		}    
    	        	        
    	        	        
    	        	        PCollection<TableRow> tarifaProducto = p.apply("Leer dbo_ttarifa_producto",
    	        	                BigQueryIO.readTableRows().from("fid-cmms-datalake-dev.sise_fid_datastrreaming.ttarifa_producto_fp_snapshot"));

    	        	            // Leer la tabla dbo_ttarifa
    	        	            PCollection<TableRow> tarifa = p.apply("Leer dbo_ttarifa",
    	        	                BigQueryIO.readTableRows().from("fid-cmms-datalake-dev.sise_fid_datastrreaming.ttarifa_fp_snapshot"));

    	        	            // Convertir a Key-Value Pair usando cod_tarifa como clave
    	        	            PCollection<KV<String, TableRow>> kvTarifaProducto = tarifaProducto
    	        	                .apply(ParDo.of(new ExtractKeyFn2("cod_tarifa")));
    	        	            PCollection<KV<String, TableRow>> kvTarifa = tarifa
    	        	                .apply(ParDo.of(new ExtractKeyFn2("cod_tarifa")));

    	        	            // Unir las tablas por cod_tarifa
    	        	            final TupleTag<TableRow> tarifaProductoTag = new TupleTag<>();
    	        	            final TupleTag<TableRow> tarifaTag = new TupleTag<>();

    	        	            PCollection<KV<String, CoGbkResult>> joinedTables3= KeyedPCollectionTuple
    	        	                .of(tarifaProductoTag, kvTarifaProducto)
    	        	                .and(tarifaTag, kvTarifa)
    	        	                .apply(CoGroupByKey.create());

    	        	            PCollection<TableRow> finalResult = joinedTables3.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
    	        	                @ProcessElement
    	        	                public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<TableRow> out) {
    	        	                    CoGbkResult result = element.getValue();

    	        	                    // Obtener todas las filas asociadas a la clave
    	        	                    Iterable<TableRow> tarifaProductos = result.getAll(tarifaProductoTag);
    	        	                    Iterable<TableRow> tarifas = result.getAll(tarifaTag);
    	        	                    Set<String> Unicos = new HashSet<>();
    	        	                    // Recorrer todas las combinaciones posibles de productos y tarifas
    	        	                    for (TableRow rowTarifaProducto : tarifaProductos) {
    	        	                        for (TableRow rowTarifa : tarifas) {
    	        	                            TableRow newRow = new TableRow();

    	        	                            // Manejo de valores nulos
    	        	                            String coberturaID = rowTarifa.containsKey("cod_tarifa") ? rowTarifa.get("cod_tarifa").toString() : "0";
    	        	                            String cobertura = rowTarifa.containsKey("txt_desc") ? rowTarifa.get("txt_desc").toString() : "";
    	        	                     //       String ramoFecuID = rowTarifa.containsKey("cod_ramo") ? rowTarifa.get("cod_ramo").toString() : "0";
    	        	                            String ramoPlanID = rowTarifaProducto.containsKey("cod_ramo") ? rowTarifaProducto.get("cod_ramo").toString() : "0";
    	        	                            String planTecnicoID = rowTarifaProducto.containsKey("cod_producto") ? rowTarifaProducto.get("cod_producto").toString() : "0";
    	        	                            if (Unicos.contains(cobertura)) {
    	        	                                continue;
    	        	                            }
    	        	                            Unicos.add(cobertura);
    	        	                            // Agregar datos a la fila de salida
    	        	                            newRow.set("GrupoCoberturaID", null);
    	        	                            newRow.set("CoberturaID", coberturaID);
    	        	                            newRow.set("Cobertura", cobertura);
    	        	                            
    	        	                         //   newRow.set("RamoFecuID", ramoFecuID);
    	        	                            newRow.set("RamoPlanID", (int) Double.parseDouble(ramoPlanID.toString()));
    	        	                            newRow.set("PlanTecnicoID", planTecnicoID);

    	        	                            out.output(newRow);
    	        	                        }
    	        	                    }
    	        	                }
    	        	            }));
    	        	            
    	        	            //Subir Tabla COBERTURA a la cloud 
	    	        	         // Insert datos MySQL
	    	        	            finalResult.apply("EscribirEnMySQL",
	    	        	        	            JdbcIO.<TableRow>write()
	    	        	        	            .withDataSourceConfiguration(
	    	        	        	            JdbcIO.DataSourceConfiguration.create(
	    	        	        	                "com.mysql.cj.jdbc.Driver", cloudSqlUrl)
	    	        	        	                .withUsername(user)
	    	        	        	                .withPassword(password))
	    	        	    .withStatement("INSERT INTO Cobertura (GrupoCoberturaId, CoberturaId, Cobertura) VALUES (?, ?, ?)")
	    	        	    .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
	    	        	        	            @Override
	    	        	        	            public void setParameters(TableRow row, PreparedStatement query) throws Exception {
	    	        	        	            Object GrupoCoberturaId = row.get("GrupoCoberturaId");
	    	        	    	                Object CoberturaID = row.get("CoberturaID");
	    	        	    	                Object Cobertura = row.get("Cobertura");
	    	        	        	                // print pre insert
	    	        	    	              
	    	        	    	                
	    	        	    	                if (GrupoCoberturaId != null) {
	    	        	    	                    query.setInt(1, Integer.parseInt(GrupoCoberturaId.toString()));
	    	        	    	                } else {
	    	        	    	                    query.setNull(1, Types.INTEGER);
	    	        	    	                }
	    	        	        	            query.setInt(2, CoberturaID != null ? (int) Double.parseDouble(CoberturaID.toString()) : 0);
	    	        	    	                query.setString(3, Cobertura != null ? Cobertura.toString() : null);
	    	        	        	            }
	    	        	        	        }));  
    	        	   
    	        	        //Inicio de creacion Tabla PRODUCTO-COBERTURA
    	        	            PCollection<KV<String, TableRow>> kvProducto = productos
        	        	                .apply(ParDo.of(new ExtractKeyFn("RamoPlanID", "PlanTecnicoID")));
        	        	            PCollection<KV<String, TableRow>> kvCobertura = finalResult
        	        	                .apply(ParDo.of(new ExtractKeyFn("RamoPlanID", "PlanTecnicoID")));
        	        	            
        	        	            final TupleTag<TableRow> ProductoTag = new TupleTag<>();
        	        	            final TupleTag<TableRow> CoberturaTag = new TupleTag<>();

        	        	            PCollection<KV<String, CoGbkResult>> joinedTables4= KeyedPCollectionTuple
        	        	                .of(ProductoTag, kvProducto)
        	        	                .and(CoberturaTag, kvCobertura)
        	        	                .apply(CoGroupByKey.create());
        	        	            
        	        	            
        	        	            
        	        	            PCollection<TableRow> ProductoCobertura = joinedTables4.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
        	        	                @ProcessElement
        	        	                public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<TableRow> out) {
        	        	                    CoGbkResult result = element.getValue();

        	        	                    // Obtener todas las filas asociadas a la clave
        	        	                    Iterable<TableRow> Producto = result.getAll(ProductoTag);
        	        	                    Iterable<TableRow> Cobertura = result.getAll(CoberturaTag);
        	        	                //    Set<String> Unicos = new HashSet<>();
        	        	                    // Recorrer todas las combinaciones posibles de productos y tarifas
        	        	                    for (TableRow rowProducto : Producto) {
        	        	                        for (TableRow rowCobertura : Cobertura) {
        	        	                            TableRow newRow = new TableRow();
        	        	                      /*      if (Unicos.contains(cobertura)) {
        	        	                                continue;
        	        	                            }
        	        	                            Unicos.add(cobertura); */
        	        	                            // Agregar datos a la fila de salida
        	        	                            newRow.set("ProductoID", rowProducto.get("ProductoId"));
        	        	                            newRow.set("CoberturaID", rowCobertura.get("CoberturaID"));

        	        	                            out.output(newRow);
        	        	                        }
        	        	                    }
        	        	                }
        	        	            }));
        	        	            
        	        	            
        	        	            
        	        	            //Subir Tabla PRODUCTO-COBERTURA a la cloud 
   	    	        	        // Insert datos MySQL
        	        	            ProductoCobertura.apply("EscribirEnMySQL",
   	    	        	        	            JdbcIO.<TableRow>write()
   	    	        	        	            .withDataSourceConfiguration(
   	    	        	        	            JdbcIO.DataSourceConfiguration.create(
   	    	        	        	                "com.mysql.cj.jdbc.Driver", cloudSqlUrl)
   	    	        	        	                .withUsername(user)
   	    	        	        	                .withPassword(password))
   	    	        	    .withStatement("INSERT INTO ProductoCobertura (CoberturaId, TipoCoberturaId, ProductoId, Monto, Deducible, Variable) VALUES (?, ?, ?, ?, ?, ?)")
   	    	        	    .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
   	    	        	        	            @Override
   	    	        	        	            public void setParameters(TableRow row, PreparedStatement query) throws Exception {
   	    	        	        	            Object CoberturaID = row.get("CoberturaID");
   	    	        	        	            Object TipoCoberturaId = null;
   	    	        	        	            Object ProductoId = row.get("ProductoID");
 	        	    	                
   	    	        	         	            query.setInt(1, CoberturaID != null ? (int) Double.parseDouble(CoberturaID.toString()) : null);
   	    	        	         	            query.setString(2, TipoCoberturaId != null ? TipoCoberturaId.toString() : null);
   	    	        	        	            query.setInt(3, ProductoId != null ? (int) Double.parseDouble(ProductoId.toString()) : 0);
   	    	        	        	            query.setNull(4, Types.INTEGER);
   	    	        	        	            query.setNull(5, Types.INTEGER);
   	    	        	        	            query.setNull(6, Types.INTEGER);

   	    	        	        	            }
   	    	        	        	        }));        
        	    	        	        	            
            
        	        	       /*     productos.apply("Convertir TableRow a String", ParDo.of(new DoFn<TableRow, String>() {
            	                        @ProcessElement
            	                        public void processElement(ProcessContext c) {
            	                        TableRow row = c.element();
            	                        String rowString = row.toString(); 
            	                        c.output(rowString); 
            	                    }
            	                    })).apply("Guardar registros en archivo", TextIO.write()
            	                        .to("./")  
            	                        .withSuffix("PipelineProductos.txt")
            	                        .withoutSharding());
        	        	            
        	        	            ProductoCobertura.apply("Convertir TableRow a String", ParDo.of(new DoFn<TableRow, String>() {
        	        	                @ProcessElement
        	        	                public void processElement(ProcessContext c) {
        	        	                TableRow row = c.element();
        	        	                String rowString = row.toString(); 
        	        	                c.output(rowString); 
        	        	            }
        	        	            })).apply("Guardar registros en archivo", TextIO.write()
        	        	                .to("./")  
        	        	                .withSuffix("PipelineProductoCobertura.txt")
        	        	                .withoutSharding());
        	        	            
        	        	            
        	        	            finalResult.apply("Convertir TableRow a String", ParDo.of(new DoFn<TableRow, String>() {
        	        	                @ProcessElement
        	        	                public void processElement(ProcessContext c) {
        	        	                TableRow row = c.element();
        	        	                String rowString = row.toString(); 
        	        	                c.output(rowString); 
        	        	            }
        	        	            })).apply("Guardar registros en archivo", TextIO.write()
        	        	                .to("./")  
        	        	                .withSuffix("PipelineCoberturas.txt")
        	        	                .withoutSharding());
        	        	            
        	        	            
        	        	            */
        	        	            p.run().waitUntilFinish();
        	        	            
        	        	            
        	        	        
    	    }

    	    static class ExtractKeyFn extends DoFn<TableRow, KV<String, TableRow>> {
    	        private final String key1;
    	        private final String key2;

    	        ExtractKeyFn(String key1, String key2) {
    	            this.key1 = key1;
    	            this.key2 = key2;
    	        }

    	        @ProcessElement
    	        public void processElement(@Element TableRow row, OutputReceiver<KV<String, TableRow>> out) {
    	            String key = row.get(key1) + "-" + row.get(key2);
    	            out.output(KV.of(key, row));
    	        }
    	    }
    	    static class ExtractKeyFn2 extends DoFn<TableRow, KV<String, TableRow>> {
    	        private final String keyField;

    	        ExtractKeyFn2(String keyField) {
    	            this.keyField = keyField;
    	        }

    	        @ProcessElement
    	        public void processElement(@Element TableRow row, OutputReceiver<KV<String, TableRow>> out) {
    	            if (row.containsKey(keyField)) {
    	                String key = row.get(keyField).toString();
    	                out.output(KV.of(key, row));
    	            }
    	        }
    	    }
    	}

     

