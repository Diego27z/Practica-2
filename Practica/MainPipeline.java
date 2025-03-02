package com.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
// Habilitar este import al momento de realizar el despliegue a DataFlow
//import org.apache.beam.runners.dataflow.DataflowRunner;

// Deshabilitar este import al momento de desplegar hacia Dataflow (Permisos GCP)
import org.apache.beam.runners.direct.DirectRunner;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class MainPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(MainPipeline.class);

    public static void main(String[] args) throws InterruptedException {
    	PipelineProductoCobertura.main(args);
    	PipelineProductoCorredor.main(args);
    }
}
