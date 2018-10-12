package com.orangutang.orangutang.service.other.kettle;


import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LogMessage;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.www.SlaveServerTransStatus;

import java.util.*;

/**
 * kettle 调度服务层
 */
public class TransExecutor implements Runnable {
    private String executionId;
    private TransExecutionConfiguration executionConfiguration;
    private TransMeta transMeta = null;
    private Trans trans = null;
    private Map<StepMeta, String> stepLogMap = new HashMap<StepMeta, String>();
    private TransSplitter transSplitter = null;
    private boolean finished = false;
    private long errCount;
    private static Hashtable<String, TransExecutor> executors = new Hashtable<String, TransExecutor>();
    public static void remove(String executionId) {
        executors.remove(executionId);
    }
    public String getExecutionId() {
        return executionId;
    }
    public long getErrCount() {
        return errCount;
    }
    public static TransExecutor getExecutor(String executionId) {
        return executors.get(executionId);
    }
    public String getCarteObjectId() {
        return carteObjectId;
    }
    public void setCarteObjectId(String carteObjectId) {
        this.carteObjectId = carteObjectId;
    }
    public TransMeta getTransMeta() {
        return transMeta;
    }
    public void setTransMeta(TransMeta transMeta) {
        this.transMeta = transMeta;
    }
    public TransExecutionConfiguration getExecutionConfiguration() {
        return executionConfiguration;
    }
    public void setExecutionConfiguration(TransExecutionConfiguration executionConfiguration) {
        this.executionConfiguration = executionConfiguration;
    }
    public Trans getTrans() {
        return trans;
    }
    public void setTrans(Trans trans) {
        this.trans = trans;
    }


    public static Hashtable<String, TransExecutor> getExecutors(){
        return executors;
    }
    private TransExecutor(TransExecutionConfiguration transExecutionConfiguration, TransMeta transMeta) {
        this.executionId = UUID.randomUUID().toString().replaceAll("-", "");
        this.executionConfiguration = transExecutionConfiguration;
        this.transMeta = transMeta;
    }

    public static synchronized TransExecutor initExecutor(TransExecutionConfiguration transExecutionConfiguration, TransMeta transMeta) {
        TransExecutor transExecutor = new TransExecutor(transExecutionConfiguration, transMeta);
        executors.put(transExecutor.getExecutionId(), transExecutor);
        return transExecutor;
    }

    /**
     *运行主方法，包括本地，远程和集群模式。
     */
    @Override
    public void run() {
        try {
            if (executionConfiguration.isExecutingLocally()) {
                // Set the variables
                transMeta.injectVariables( executionConfiguration.getVariables() );
                // Set the named parameters
                Map<String, String> paramMap = executionConfiguration.getParams();
                Set<String> keys = paramMap.keySet();
                for (String key : keys) {
                    transMeta.setParameterValue(key, Const.NVL(paramMap.get(key), ""));
                }
                transMeta.activateParameters();
                // Set the arguments
                Map<String, String> arguments = executionConfiguration.getArguments();
                String[] argumentNames = arguments.keySet().toArray( new String[arguments.size()] );
                Arrays.sort( argumentNames );

                String[] args = new String[argumentNames.length];
                for ( int i = 0; i < args.length; i++ ) {
                    String argumentName = argumentNames[i];
                    args[i] = arguments.get( argumentName );
                }
                boolean initialized = false;
                trans = new Trans( transMeta );

                trans.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
                trans.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );
                trans.setLogLevel( executionConfiguration.getLogLevel() );
                trans.setReplayDate( executionConfiguration.getReplayDate() );
                trans.setRepository( executionConfiguration.getRepository() );
                try {
                    trans.prepareExecution( args );
                    capturePreviewData(trans, transMeta.getSteps());
                    initialized = true;
                } catch (Exception e ) {
                    e.printStackTrace();
                    checkErrorVisuals();
                    throw new Exception("trans 任务初始化失败");
                }
                if ( trans.isReadyToStart() && initialized) {
                    trans.addTransListener(new TransAdapter() {
                        public void transFinished(Trans trans) {
                            checkErrorVisuals();
                        }
                    });
                    trans.startThreads();

                    while(!trans.isFinished())
                        Thread.sleep(500);
                    errCount = trans.getErrors();
                } else {
                    checkErrorVisuals();
                    errCount = trans.getErrors();
                }
            } else if (executionConfiguration.isExecutingRemotely()) {
                carteObjectId = Trans.sendToSlaveServer( transMeta, executionConfiguration, App.getInstance().getRepository(), App.getInstance().getMetaStore() );
                SlaveServer remoteSlaveServer = executionConfiguration.getRemoteServer();
                boolean running = true;
                while(running) {
                    SlaveServerTransStatus transStatus = remoteSlaveServer.getTransStatus(transMeta.getName(), carteObjectId, 0);
                    running = transStatus.isRunning();
                    if(!running) errCount = transStatus.getResult().getNrErrors();
                    Thread.sleep(500);
                }
            } else if(executionConfiguration.isExecutingClustered()) {
                transSplitter = new TransSplitter( transMeta );
                transSplitter.splitOriginalTransformation();

                for (String var : Const.INTERNAL_TRANS_VARIABLES) {
                    executionConfiguration.getVariables().put(var, transMeta.getVariable(var));
                }
                for (String var : Const.INTERNAL_JOB_VARIABLES) {
                    executionConfiguration.getVariables().put(var, transMeta.getVariable(var));
                }

                // Parameters override the variables.
                // For the time being we're passing the parameters over the wire
                // as variables...

                TransMeta ot = transSplitter.getOriginalTransformation();
                for (String param : ot.listParameters()) {
                    String value = Const.NVL(ot.getParameterValue(param), Const.NVL(ot.getParameterDefault(param), ot.getVariable(param)));
                    if (!Const.isEmpty(value)) {
                        executionConfiguration.getVariables().put(param, value);
                    }
                }

                try {
                    Trans.executeClustered(transSplitter, executionConfiguration);
                } catch (Exception e) {
                    // Something happened posting the transformation to the
                    // cluster.
                    // We need to make sure to de-allocate ports and so on for
                    // the next try...
                    // We don't want to suppress original exception here.
                    try {
                        Trans.cleanupCluster(App.getInstance().getLog(), transSplitter);
                    } catch (Exception ee) {
                        throw new Exception("Error executing transformation and error to clenaup cluster", e);
                    }
                    // we still have execution error but cleanup ok here...
                    throw e;
                }


                Trans.monitorClusteredTransformation(App.getInstance().getLog(), transSplitter, null);


                Result result = Trans.getClusteredTransformationResult(App.getInstance().getLog(), transSplitter, null);
                errCount = result.getNrErrors();
            }

        } catch(Exception e) {
           e.printStackTrace();
        } finally {
            // do something here!
        }
    }

    public void capturePreviewData(Trans trans, List<StepMeta> stepMetas) {
        final StringBuffer loggingText = new StringBuffer();

        try {
            final TransMeta transMeta = trans.getTransMeta();

            for (final StepMeta stepMeta : stepMetas) {
                final RowMetaInterface rowMeta = transMeta.getStepFields( stepMeta ).clone();
                previewMetaMap.put(stepMeta, rowMeta);
                final List<Object[]> rowsData = new LinkedList<Object[]>();

                previewDataMap.put(stepMeta, rowsData);
                previewLogMap.put(stepMeta, loggingText);

                StepInterface step = trans.findRunThread(stepMeta.getName());

                if (step != null) {

                    step.addRowListener(new RowAdapter() {
                        @Override
                        public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
                            try {
                                rowsData.add(rowMeta.cloneRow(row));
                                if (rowsData.size() > 100) {
                                    rowsData.remove(0);
                                }
                            } catch (Exception e) {
                                throw new KettleStepException("Unable to clone row for metadata : " + rowMeta, e);
                            }
                        }
                    });
                }

            }
        } catch (Exception e) {
            loggingText.append(Const.getStackTracker(e));
        }

        trans.addTransListener(new TransAdapter() {
            @Override
            public void transFinished(Trans trans) throws KettleException {
                if (trans.getErrors() != 0) {
                    for (StepMetaDataCombi combi : trans.getSteps()) {
                        if (combi.copy == 0) {
                            StringBuffer logBuffer = KettleLogStore.getAppender().getBuffer(combi.step.getLogChannel().getLogChannelId(), false);
                            previewLogMap.put(combi.stepMeta, logBuffer);
                        }
                    }
                }
            }
        });
    }

    protected Map<StepMeta, RowMetaInterface> previewMetaMap = new HashMap<StepMeta, RowMetaInterface>();
    protected Map<StepMeta, List<Object[]>> previewDataMap = new HashMap<StepMeta, List<Object[]>>();
    protected Map<StepMeta, StringBuffer> previewLogMap = new HashMap<StepMeta, StringBuffer>();

    private void checkErrorVisuals() {
        if (trans.getErrors() > 0) {
            stepLogMap.clear();

            for (StepMetaDataCombi combi : trans.getSteps()) {
                if (combi.step.getErrors() > 0) {
                    String channelId = combi.step.getLogChannel().getLogChannelId();
                    List<KettleLoggingEvent> eventList = KettleLogStore.getLogBufferFromTo(channelId, false, 0, KettleLogStore.getLastBufferLineNr());
                    StringBuilder logText = new StringBuilder();
                    for (KettleLoggingEvent event : eventList) {
                        Object message = event.getMessage();
                        if (message instanceof LogMessage) {
                            LogMessage logMessage = (LogMessage) message;
                            if (logMessage.isError()) {
                                logText.append(logMessage.getMessage()).append(Const.CR);
                            }
                        }
                    }
                    stepLogMap.put(combi.stepMeta, logText.toString());
                }
            }

        } else {
            stepLogMap.clear();
        }
    }

    private String carteObjectId = null;

    public boolean isFinished() {
        return finished;
    }


    public void stop() {
        if(trans!=null){
            trans.stopAll();
        }
    }

    public void pause() {
        if(!trans.isPaused())
            trans.pauseRunning();
        else
            trans.resumeRunning();
    }

}