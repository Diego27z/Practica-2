package com.pipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.pipeline.PipelineProductoCobertura.ExtractKeyFn2;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineProductoCorredor {
	private static final Logger LOG = LoggerFactory.getLogger(PipelineProductoCorredor.class);
	
	public static void main(String[] args) throws InterruptedException {
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
        Pipeline p = Pipeline.create(options);
    //Obtener Tabla PRODUCTOS desde la cloud
     
     String cloudSqlUrl = "";
     String user = "";
     String password = "";
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
 	        	//---INICIO PIPELINE CORREDORES----------------------------------------------------  
        	        
        	//-------------------------------------------------------        
        	        System.out.println("----------------------------------------------------------------------------------");
        	//-------------------------------------------------------   
        	        
        	        
        	        BigQuery bigqueryCorredor = BigQueryOptions.getDefaultInstance().getService();

            		// Lista de tablas origen y sus respectivos snapshots
            		List<String> tablesCorredor = Arrays.asList(
            				"fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_cial_comis_fp",
            				"fid-cmms-datalake-dev.sise_fid_datastrreaming.magente_fp",
            				"fid-cmms-datalake-dev.sise_fid_datastrreaming.mpersona_fp"
            		);

            		// Crear snapshots para cada tabla
            		for (String sourceTable : tablesCorredor) {
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
            			Job job = bigqueryCorredor.create(JobInfo.newBuilder(queryConfig).build());
            			job = job.waitFor();
            			if (job.isDone()) {
            				System.out.println("Snapshot creado: " + snapshotTable);
            			} else {
            				System.out.println("Error al crear snapshot: " + job.getStatus().getError());
            			}
            		}    
        	        
        	        
        	        // Leer las tablas desde BigQuery
        	        PCollection<TableRow> productoComis = p.apply("Leer Producto Comis",
        	                BigQueryIO.readTableRows().from("fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_cial_comis_fp_snapshot"));

        	        PCollection<TableRow> agentes = p.apply("Leer Agentes",
        	                BigQueryIO.readTableRows().from("fid-cmms-datalake-dev.sise_fid_datastrreaming.magente_fp_snapshot"));

        	        PCollection<TableRow> personas = p.apply("Leer Personas",
        	                BigQueryIO.readTableRows().from("fid-cmms-datalake-dev.sise_fid_datastrreaming.mpersona_fp_snapshot"));

        	        // Convertir a Key-Value Pairs para JOIN
        	        PCollection<KV<String, TableRow>> kvProductoComis = productoComis.apply(ParDo.of(new ExtractKeyFn("cod_agente", "cod_tipo_agente")));
        	        PCollection<KV<String, TableRow>> kvAgentes = agentes.apply(ParDo.of(new ExtractKeyFn("cod_agente", "cod_tipo_agente")));
        	        PCollection<KV<String, TableRow>> kvPersonas = personas.apply(ParDo.of(new ExtractKeyFn("id_persona", "")));

        	        // JOIN de las tablas
        	        final TupleTag<TableRow> productoComisTag = new TupleTag<>();
        	        final TupleTag<TableRow> agentesTag = new TupleTag<>();
        	        final TupleTag<TableRow> personasTag = new TupleTag<>();
        	        final TupleTag<TableRow> agentesIDTag = new TupleTag<>();

        	        PCollection<KV<String, CoGbkResult>> joinedData = KeyedPCollectionTuple
        	                .of(productoComisTag, kvProductoComis)
        	                .and(agentesTag, kvAgentes)
        	                .apply(CoGroupByKey.create());

        	        // Transformar y mapear los datos finales
        	        PCollection<TableRow> finalTable3 = joinedData.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
        	            @ProcessElement
        	            public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<TableRow> out) {
        	                CoGbkResult result = element.getValue();
        	                Iterable<TableRow> rowsProductoComis = result.getAll(productoComisTag);
        	                Iterable<TableRow> rowsAgente = result.getAll(agentesTag);

        	                for (TableRow rowProductoComis : rowsProductoComis) {
        	                    for (TableRow rowAgente : rowsAgente) {
        	                        
        	                            TableRow newRow = new TableRow();
        	                            newRow.set("id_persona", rowAgente.get("id_persona"));
        	                            newRow.set("pje_comision", rowProductoComis.get("pje_comision"));
        	                            newRow.set("cod_agente", rowAgente.get("cod_agente"));
        	                            newRow.set("cod_producto", rowProductoComis.get("cod_producto"));
        	                            newRow.set("cod_ramo", rowProductoComis.get("cod_ramo"));

        	                            out.output(newRow);
        	                        
        	                    }
        	                }        
        	            }
        	        }));
        	        
        	        PCollection<KV<String, TableRow>> kvAgentesID = finalTable3.apply(ParDo.of(new ExtractKeyFn("id_persona", "")));
        	        
        	        PCollection<KV<String, CoGbkResult>> joinedData2 = KeyedPCollectionTuple
        	                .of(personasTag, kvPersonas)
        	                .and(agentesIDTag, kvAgentesID)
        	                .apply(CoGroupByKey.create());
        	        
        	        PCollection<TableRow> finalTable4 = joinedData2.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
        	            @ProcessElement
        	            public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<TableRow> out) {
        	                CoGbkResult result = element.getValue();
        	                Iterable<TableRow> rowskvPersonas = result.getAll(personasTag);
        	                Iterable<TableRow> rowskvAgentesID = result.getAll(agentesIDTag);
        	                
        	                for (TableRow rowkvPersonas : rowskvPersonas) {
        	                    for (TableRow rowkvAgentesID : rowskvAgentesID) {
        	                        
        	                            TableRow newRow = new TableRow();
        	                            
        	                            // Procesar nro_nit
        	                            String nroNit = rowkvPersonas.containsKey("nro_nit") ? (String) rowkvPersonas.get("nro_nit") : "";
        	                            String corredorID = !nroNit.isEmpty() ? nroNit.substring(0, nroNit.length() - 1) : "";
        	                            String corredorDV = !nroNit.isEmpty() ? nroNit.substring(nroNit.length() - 1).toUpperCase() : "";

        	                            // Procesar Nombre del Corredor
        	                            String nombre = rowkvPersonas.containsKey("txt_nombre") ? (String) rowkvPersonas.get("txt_nombre") : "";
        	                            String apellido1 = rowkvPersonas.containsKey("txt_apellido1") ? (String) rowkvPersonas.get("txt_apellido1") : "";
        	                            String apellido2 = rowkvPersonas.containsKey("txt_apellido2") ? (String) rowkvPersonas.get("txt_apellido2") : "";

        	                            String nombreCorredor = nombre.trim() + " " + apellido1.trim() + " " + apellido2.trim();

        	                            newRow.set("AgenteID", rowkvAgentesID.get("cod_agente"));
        	                            newRow.set("CorredorID", corredorID);
        	                            newRow.set("CorredorDV", corredorDV);
        	                            newRow.set("NombreCorredor", nombreCorredor.toUpperCase());
        	                            //-------//
        	                            newRow.set("PorcentajeComision", rowkvAgentesID.get("pje_comision"));       
        	                            newRow.set("ProductoComercialID", (int) Double.parseDouble(rowkvAgentesID.get("cod_producto").toString()));     	                            
        	                            newRow.set("RamoComercialID", (int) Double.parseDouble(rowkvAgentesID.get("cod_ramo").toString()));

        	                            out.output(newRow);
        	                        
        	                    }
        	                }

        	                
        	            }
        	        }));
        	        

        	     // Insert datos MySQL
        	        finalTable4.apply("EscribirEnMySQL",
        	    	            JdbcIO.<TableRow>write()
        	    	            .withDataSourceConfiguration(
        	    	            JdbcIO.DataSourceConfiguration.create(
        	    	                "com.mysql.cj.jdbc.Driver", cloudSqlUrl)
        	    	                .withUsername(user)
        	    	                .withPassword(password))
        	    	            .withStatement("INSERT INTO Corredor (AgenteID, CorredorID, CorredorDV, NombreCorredor) " +
        	    	                    "VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE CorredorDV = ?, NombreCorredor = ?")

        	    	            .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
        	    	                @Override
        	    	                public void setParameters(TableRow row, PreparedStatement query) throws Exception {
        	    	                    try {
        	    	                        Object AgenteID = row.get("AgenteID");
        	    	                        Object CorredorID = row.get("CorredorID");
        	    	                        Object CorredorDV = row.get("CorredorDV");
        	    	                        Object NombreCorredor = row.get("NombreCorredor");

        	    	                        int agenteIDValue = isNumeric(AgenteID) ? (int) Double.parseDouble(AgenteID.toString()) : 0;
        	    	                        int corredorIDValue = isNumeric(CorredorID) ? (int) Double.parseDouble(CorredorID.toString()) : 0;
        	    	                        int corredorDVValue = isNumeric(CorredorDV) ? (int) Double.parseDouble(CorredorDV.toString()) : 0;

        	    	                        query.setInt(1, agenteIDValue);
        	    	                        query.setInt(2, corredorIDValue);
        	    	                        query.setInt(3, corredorDVValue);
        	    	                        query.setString(4, NombreCorredor != null ? NombreCorredor.toString() : null);
        	    	                        query.setInt(5, corredorDVValue);
        	    	                        query.setString(6, NombreCorredor != null ? NombreCorredor.toString() : null);
        	    	                    } catch (SQLException e) {
        	    	                        // Maneja el error aquí si algo sale mal
        	    	                        e.printStackTrace();
        	    	                    }

        	    }

        	    // Función auxiliar para validar si un valor es numérico
        	    private boolean isNumeric(Object obj) {
        	        if (obj == null) return false;
        	        try {
        	            Double.parseDouble(obj.toString());
        	            return true;
        	        } catch (NumberFormatException e) {
        	            return false;
        	        }
        	    }
        	}));

        	    	        
        	 


        	        PCollection<KV<String, TableRow>> kvProducto = productos
        	                .apply(ParDo.of(new ExtractKeyFn("RamoComercialID", "ProductoComercialID")));
        	            PCollection<KV<String, TableRow>> kvCorredor = finalTable4
        	                .apply(ParDo.of(new ExtractKeyFn("RamoComercialID", "ProductoComercialID")));
        	            
        	            final TupleTag<TableRow> ProductoTag = new TupleTag<>();
        	            final TupleTag<TableRow> CorredorTag = new TupleTag<>();

        	            PCollection<KV<String, CoGbkResult>> joinedTables4 = KeyedPCollectionTuple
        	                .of(ProductoTag, kvProducto)
        	                .and(CorredorTag, kvCorredor)
        	                .apply(CoGroupByKey.create());
        	        
        	            
        	            PCollection<TableRow> ProductoCorredor = joinedTables4.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
        	                @ProcessElement
        	                public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<TableRow> out) {
        	                    CoGbkResult result = element.getValue();

        	                    // Obtener todas las filas asociadas a la clave
        	                    Iterable<TableRow> Producto = result.getAll(ProductoTag);
        	                    Iterable<TableRow> Corredor = result.getAll(CorredorTag);
        	                //    Set<String> Unicos = new HashSet<>();
        	                    // Recorrer todas las combinaciones posibles de productos y tarifas
        	                    for (TableRow rowProducto : Producto) {
        	                        for (TableRow rowCorredor : Corredor) {
        	                            TableRow newRow = new TableRow();
        	                      /*      if (Unicos.contains(cobertura)) {
        	                                continue;
        	                            }
        	                            Unicos.add(cobertura); */
        	                            // Agregar datos a la fila de salida
        	                            newRow.set("ProductoId", rowProducto.get("ProductoId"));
        	                            newRow.set("CorredorID", rowCorredor.get("AgenteID"));
        	                            newRow.set("Comision", rowCorredor.get("PorcentajeComision"));
        	                            newRow.set("EstadoHabilitado", rowProducto.get("EstadoHabilitado"));
        	                            newRow.set("ProductoCorredorID", null);
        	                            newRow.set("TipoAgenteID", null);

        	                            out.output(newRow);
        	                        }
        	                    }
        	                }
        	            }));
        	            	            
        	            
        	            
        	            ProductoCorredor.apply("EscribirEnMySQL",
        	    	            JdbcIO.<TableRow>write()
        	    	            .withDataSourceConfiguration(
        	    	            JdbcIO.DataSourceConfiguration.create(
        	    	                "com.mysql.cj.jdbc.Driver", cloudSqlUrl)
        	    	                .withUsername(user)
        	    	                .withPassword(password))
        	.withStatement("INSERT INTO ProductoCorredor (ProductoId, CorredorId, Comision, EstadoHabilitado, TipoAgenteId) VALUES (?, ?, ?, ?, ?)")
        	.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
        	    	            @Override
        	    	            public void setParameters(TableRow row, PreparedStatement query) throws Exception {
        		                try{ Object ProductoId = row.get("ProductoId");
        		                Object CorredorId = row.get("CorredorID");
        		                Object Comision = row.get("Comision");
        		                Object EstadoHabilitado = row.get("EstadoHabilitado");

        		                
        	     	            query.setInt(1, ProductoId != null ? (int) Double.parseDouble(ProductoId.toString()) : 0);
        	    	            query.setInt(2, CorredorId != null ? (int) Double.parseDouble(CorredorId.toString()) : 0);
        	    	            query.setInt(3, Comision != null ? ((int) Double.parseDouble(Comision.toString()))/1000000 : 0);
        	    	            query.setInt(4, EstadoHabilitado != null ? (int) Double.parseDouble(EstadoHabilitado.toString()) : 0);
        	    	            query.setNull(5, Types.INTEGER);
        	    	            }catch (SQLException e) {
	    	                        // Maneja el error aquí si algo sale mal
	    	                        e.printStackTrace();
	    	                    }
        	    	        }}));   
        	            
        	            
        	            
        	   /*         ProductoCorredor.apply("Convertir TableRow a String", ParDo.of(new DoFn<TableRow, String>() {
            	            @ProcessElement
            	            public void processElement(ProcessContext c) {
            	                TableRow row = c.element();
            	                String rowString = row.toString(); 
            	                c.output(rowString); 
            	            }
            	        }))
            	        .apply("Guardar registros en archivos", TextIO.write()
            	            .to("./")  // Prefijo del archivo
            	            .withSuffix("-.txt")           // Extensión de los archivos
            	            .withNumShards(5));           // Número de archivos
        	            
        	            
        	            
        	        finalTable4.apply("Convertir TableRow a String", ParDo.of(new DoFn<TableRow, String>() {
        	            @ProcessElement
        	            public void processElement(ProcessContext c) {
        	                TableRow row = c.element();
        	                String rowString = row.toString(); 
        	                c.output(rowString); 
        	            }
        	        }))
        	        .apply("Guardar registros en archivos", TextIO.write()
        	            .to("./")  // Prefijo del archivo
        	            .withSuffix("-PipelineCorredores.txt")           // Extensión de los archivos
        	            .withNumShards(5));           // Número de archivos
        	            
        	            productos.apply("Convertir TableRow a String", ParDo.of(new DoFn<TableRow, String>() {
        	                @ProcessElement
        	                public void processElement(ProcessContext c) {
        	                TableRow row = c.element();
        	                String rowString = row.toString(); 
        	                c.output(rowString); 
        	            }
        	            })).apply("Guardar registros en archivo", TextIO.write()
        	                .to("./")  
        	                .withSuffix("ProductosCLOUD.txt")
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
