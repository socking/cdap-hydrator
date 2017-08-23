package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.*;
import co.cask.cdap.api.dataset.lib.partitioned.*;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcInputFormat;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Batch Source that reads from a FileSet that has its data formatted as text.
 *
 * LongWritable is the first parameter because that is the key used by Hadoop's {@link TextInputFormat}.
 * Similarly, Text is the second parameter because that is the value used by Hadoop's {@link TextInputFormat}.
 * {@link StructuredRecord} is the third parameter because that is what the source will output.
 * All the plugins included with Hydrator operate on StructuredRecord.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(PFSORCSource.NAME)
@Description("Reads from a FileSet that has its data formatted as text.")
public class PFSORCSource extends BatchSource<NullWritable,OrcStruct, StructuredRecord> {
    public static final String NAME = "PFSORC";
    private final Conf config;
    private List<PartitionDetail> partitions;
    //public static String CONSUMER_STATE_TABLE = "TextFileSetSteppingSourceStateTable";
    PartitionConsumer partitionConsumer;
    private Schema OUTPUT_SCHEMA=null;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PFSORCSource.class);
    /**
     * Config properties for the plugin.
     */

    public static class Conf extends PluginConfig {
        public static final String FILESET_NAME = "fileSetName";
        public static final String CREATE_IF_NOT_EXISTS = "createIfNotExists";
        public static final String DELETE_INPUT_ON_SUCCESS = "deleteInputOnSuccess";

        // The name annotation tells CDAP what the property name is. It is optional, and defaults to the variable name.
        // Note:  only primitives (including boxed types) and string are the types that are supported
        @Name(FILESET_NAME)
        @Description("The name of the FileSet to read from.")
        private String fileSetName;

        @Nullable
        @Description("fileset basepath.")
        private String basePath;

        // A nullable fields tells CDAP that this is an optional field.
        @Nullable
        @Name(CREATE_IF_NOT_EXISTS)
        @Description("Whether to create the FileSet if it doesn't already exist. Defaults to false.")
        private Boolean createIfNotExists;

        @Nullable
        @Name("partitionConsumerLimit")
        @Description("Comsumer limits on number of partitions to be processed.")
        private String partitionConsumerLimit;

        @Nullable
        @Name(DELETE_INPUT_ON_SUCCESS)
        @Description("Whether to delete the data read by the source after the run succeeds. Defaults to false.")
        private Boolean deleteInputOnSuccess;

        @Name("schemaDefinition")
        @Description("Define schema for output")
        private String schemaDefinition;

        @Nullable
        @Name("STATE_TABLE")
        @Description("CONSUMER_STATE_TABLE")
        private String CONSUMER_STATE_TABLE;

        @Nullable
        @Name("STATE_ROW_KEY")
        @Description("CONSUMER_STATE_TABLE_ROW_KEY")
        private String CONSUMER_STATE_TABLE_ROW_KEY;

        // Use a no-args constructor to set field defaults.
        public Conf() {
            fileSetName = "";
            basePath="";
            createIfNotExists = false;
            deleteInputOnSuccess = false;
            partitionConsumerLimit = "100"; //1G
            CONSUMER_STATE_TABLE = "TextFileSetSteppingSourceStateTable";
            CONSUMER_STATE_TABLE_ROW_KEY = "state";
        }
    }

    // CDAP will pass in a config with its fields populated based on the configuration given when creating the pipeline.
    public PFSORCSource(Conf config) {
        this.config = config;
    }

    // configurePipeline is called exactly once when the pipeline is being created.
    // Any static configuration should be performed here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

        // if the user has set createIfNotExists to true, create the FileSet here.
        if (config.createIfNotExists) {
            PartitionedFileSetProperties.Builder b=
                    PartitionedFileSetProperties.builder();

            b.setInputFormat(OrcInputFormat.class)
                    .setOutputFormat(TextOutputFormat.class)
                    .setEnableExploreOnCreate(true)
                    .setExploreFormat("text")
                    .setExploreSchema("text string")
                    .setDataExternal(true);

            if(!config.basePath.isEmpty()) {
                b.setBasePath(config.basePath);
            }

            pipelineConfigurer.createDataset(config.fileSetName,
                    PartitionedFileSet.class, b.build()
            );
        }

        try {
            OUTPUT_SCHEMA = Schema.parseJson(config.schemaDefinition);
            LOG.info("config.schemaDefinition:"+config.schemaDefinition);
            LOG.info("OUTPUT_SCHEMA:"+OUTPUT_SCHEMA.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }

        pipelineConfigurer.createDataset(config.CONSUMER_STATE_TABLE, KeyValueTable.class);
        pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
    }

    // prepareRun is called before every pipeline run, and is used to configure what the input should be,
    // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
    @Override
    public void prepareRun(BatchSourceContext context) throws IOException {

        ConsumerConfiguration cfg= ConsumerConfiguration.builder()
                .setTimeout(360000)
                .setMaxRetries(100)
                .build();

        PartitionedFileSet partitionedFileSet = context.getDataset(config.fileSetName);
        DatasetStatePersistor datasetStatePersistor =
                new KVTableStatePersistor(config.CONSUMER_STATE_TABLE,
                        config.CONSUMER_STATE_TABLE_ROW_KEY);
        StatePersistor statePersistor = new DelegatingStatePersistor(context, datasetStatePersistor);
        partitionConsumer = new ConcurrentPartitionConsumer(partitionedFileSet, statePersistor,cfg);

        int limit = Integer.parseInt(config.partitionConsumerLimit);
        List<PartitionDetail> mypartitions =
                partitionConsumer.consumePartitions(limit).getPartitions();
        partitions= Collections.unmodifiableList(mypartitions);
        Map<String, String> inputArgs = new HashMap();
        PartitionedFileSetArguments.addInputPartitions(inputArgs, partitions);
        context.setInput(Input.ofDataset(config.fileSetName, inputArgs));
    }

    // onRunFinish is called at the end of the pipeline run by the client that submitted the batch job.
    @Override
    public void onRunFinish(boolean succeeded, BatchSourceContext context) {
        // perform any actions that should happen at the end of the run.
        // in our case, we want to delete the data read during this run if the run succeeded.
        partitionConsumer.onFinish(partitions, succeeded);
        //else {partitionConsumer.onFinish(partitions,false);}
    }

    // initialize is called by each job executor before any call to transform is made.
    // This occurs at the start of the batch job run, after the job has been successfully submitted.
    // For example, if mapreduce is the execution engine, each mapper will call initialize at the start of the program.
    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        super.initialize(context);
        // create any resources required by transform()

    /* */
        try {
            OUTPUT_SCHEMA = Schema.parseJson(config.schemaDefinition);
            LOG.info("config.schemaDefinition:"+config.schemaDefinition);
            LOG.info("OUTPUT_SCHEMA:"+OUTPUT_SCHEMA.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }

    /**/

    }

    // destroy is called by each job executor at the end of its life.
    // For example, if mapreduce is the execution engine, each mapper will call destroy at the end of the program.
    @Override
    public void destroy() {
        // clean up any resources created by initialize

    }

    // transform is used to transform the key-value pair output by the input into objects output by this source.
    // The output should be a StructuredRecord if you want the source to be compatible with the plugins included
    // with Hydrator.
    /**
     * Read the OrcStruct and write to StructuredRecord
     * @param input
     * @param emitter
     * @throws Exception
     */
    @Override
    public void transform(KeyValue<NullWritable, OrcStruct> input, Emitter<StructuredRecord> emitter) throws Exception {
        OrcStruct orc=input.getValue();
        StructuredRecord record=transformFromORC(input.getValue(),OUTPUT_SCHEMA);
        emitter.emit (record);
    }

    private final Map<Schema, TypeDescription> schemaCache = new HashMap<>();

    /**
     * Read from OrcStruct and add the value to StructuredRecord.
     * In other words, convert OrcStruct to StructuredRecord.
     * @param orc
     * @param output_schema
     * @return
     */
    public StructuredRecord transformFromORC(OrcStruct orc, Schema output_schema) {
        List<Schema.Field> fields = output_schema.getFields();

        StructuredRecord.Builder builder= StructuredRecord.builder(output_schema);
        //int num=orc.getNumFields();
        //List<TypeDescription> types= orc.getSchema().getChildren();
        for(Schema.Field field:fields) {
            WritableComparable val=orc.getFieldValue(field.getName());

            try {
                builder.set(field.getName(),getValuefromOrcVal(field,val));
            } catch (UnsupportedTypeException e) {
                e.printStackTrace();
                LOG.info(e.getMessage());
            }
        }
        return builder.build();
    }

    private Object getValuefromOrcVal(Schema.Field field, WritableComparable val)
            throws UnsupportedTypeException {
        //Object fieldVal = input.get(field.getName());
        Schema fieldSchema = field.getSchema();
        Schema.Type fieldType = fieldSchema.getType();
        if(val != null) {
            LOG.info(fieldType.toString() + ">>>" + val.toString());
        } else {
            LOG.info(fieldType.toString() + " val is null.");
            return "";
        }
        switch (fieldType) {
            case NULL:
                return null;
            case STRING:
                return val.toString();
            case ENUM:
                return val.toString();
            case UNION:
                return val.toString();
            case BOOLEAN:
                return ((BooleanWritable) val).get()+"";
            case INT:
                return ((IntWritable)val).get()+"";
            case LONG:
                return ((LongWritable)val).get()+"";
            case FLOAT:
                return ((FloatWritable)val).get()+"";
            case DOUBLE:
                return ((DoubleWritable)val).get()+"";
            case BYTES:
                return ((BytesWritable)val).getBytes()+"";

            default:
                throw new UnsupportedTypeException(String.format("%s type is currently not supported in ORC",
                        field.getSchema().getType().name()));
        }
    }

    public static void main1(String ...a) {
        String s="2017-03-03@00:17:01|||||264|||||||||||[]|425901646278595|[1234567890]|099624612|16||19|||[]|[]|[]||||||||||||[11]|";
        String ss[] = s.split(Pattern.quote("|"),-1);
        for(int i =0 ;i< ss.length;i++)System.out.println(i+">"+ss[i]);
        System.out.println("ss.len="+ss.length);
    }

    public static void main(String ...a) throws IOException {
        File f= new File("/Users/jw769d/git/hydrator/hydrator420/schema/y");
        String str=null;

        FileReader reader = new FileReader(f);
        BufferedReader reader1=new BufferedReader(reader);
        while (true) {
            String ss =reader1.readLine();
            if (ss == null) break;
            str+=ss;
        }

        System.out.println("str="+str);
        Schema OUTPUT_SCHEMA = Schema.parseJson(str);
    }

}


