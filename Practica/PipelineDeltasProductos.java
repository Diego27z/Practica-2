package com.deltas;


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
import com.pipeline.PipelineProductoCobertura;
import com.google.cloud.bigquery.*;
//import com.pipeline.PipelineProductoCobertura.ExtractKeyFn;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.List;

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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
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

public class PipelineDeltasProductos {
	
	private static final Logger LOG = LoggerFactory.getLogger(PipelineDeltasProductos.class);

	public static void main(String[] args) throws InterruptedException  {


    	
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

		// Crear el pipeline
		Pipeline pipeline = Pipeline.create(options);
		
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		// Lista de tablas origen y sus respectivos snapshots
		List<String> tables = Arrays.asList(
				"fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_cial",
				"fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_tec_cial",
				"fid-cmms-datalake-dev.sise_fid_datastrreaming.tiso_producto"
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
            .from("fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_cial_snapshot_new"));
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
            .from("fid-cmms-datalake-dev.sise_fid_datastrreaming.te_producto_tec_cial_fp_snapshot_new"));

        PCollection<TableRow> tableRows3 = pipeline.apply("Leer Tabla 3", BigQueryIO.readTableRows()
            .from("fid-cmms-datalake-dev.sise_fid_datastrreaming.tiso_producto_fp_snapshot_new"));

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





		//Subir Tabla PRODUCTOS a la cloud
		// credencialesDB
		
		// Insert datos MySQL

		String cloudSqlUrl = "";
		String userBD = "";
		String passwordBD = "";

		finalTable2.apply("EscribirEnMySQL",
		            JdbcIO.<TableRow>write()
		            .withDataSourceConfiguration(
		            JdbcIO.DataSourceConfiguration.create(
		                "com.mysql.cj.jdbc.Driver", cloudSqlUrl)
		                .withUsername(userBD)
		                .withPassword(passwordBD))
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
}
