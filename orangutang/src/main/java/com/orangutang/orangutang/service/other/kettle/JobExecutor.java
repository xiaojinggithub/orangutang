package com.orangutang.orangutang.service.other.kettle;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.www.SlaveServerJobStatus;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 可以参考源码中的JobExecutor类。重写JobExecutor方法
 */
public class JobExecutor implements Runnable {
    private String username;
    private String executionId;
    private JobExecutionConfiguration executionConfiguration;
    private JobMeta jobMeta = null;
    private String carteObjectId = null;
    private Job job = null;
    //private static final Class PKG = JobEntryCopyResult.class;
    private boolean finished = false;
    private long errCount = 0;
    private static Hashtable<String, JobExecutor> executors = new Hashtable<String, JobExecutor>();
    //	private Map<StepMeta, String> stepLogMap = new HashMap<StepMeta, String>();
    public boolean isFinished() {
        return finished;
    }
    public Job getJob() {
        return job;
    }
    public String getExecutionId() {
        return executionId;
    }
    public static JobExecutor getExecutor(String executionId) {
        return executors.get(executionId);
    }
    public void setJob(Job job) {
        this.job = job;
    }
    public JobExecutionConfiguration getExecutionConfiguration() {
        return executionConfiguration;
    }
    public void setExecutionConfiguration(JobExecutionConfiguration executionConfiguration) {
        this.executionConfiguration = executionConfiguration;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getCarteObjectId() {
        return carteObjectId;
    }
    public JobMeta getJobMeta() {
        return jobMeta;
    }
    public void setJobMeta(JobMeta jobMeta) {
        this.jobMeta = jobMeta;
    }
    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public long getErrCount() {
        return errCount;
    }
    public static Hashtable<String, JobExecutor> getExecutors(){
        return executors;
    }
    public static void remove(String executionId) {
        executors.remove(executionId);
    }
    public JobExecutor(JobExecutionConfiguration executionConfiguration, JobMeta jobMeta) {
        this.executionId = UUID.randomUUID().toString().replaceAll("-", "");
        this.executionConfiguration = executionConfiguration;
        this.jobMeta = jobMeta;
    }


    public static synchronized JobExecutor initExecutor(JobExecutionConfiguration executionConfiguration, JobMeta jobMeta) {
        JobExecutor jobExecutor = new JobExecutor(executionConfiguration, jobMeta);
        executors.put(jobExecutor.getExecutionId(), jobExecutor);
        return jobExecutor;
    }

    @Override
    public void run() {
        try {
            for (String varName : executionConfiguration.getVariables().keySet()) {
                String varValue = executionConfiguration.getVariables().get(varName);
                jobMeta.setVariable(varName, varValue);
            }

            for (String paramName : executionConfiguration.getParams().keySet()) {
                String paramValue = executionConfiguration.getParams().get(paramName);
                jobMeta.setParameterValue(paramName, paramValue);
            }
            if (executionConfiguration.isExecutingLocally()) {
                SimpleLoggingObject spoonLoggingObject = new SimpleLoggingObject( "SPOON", LoggingObjectType.SPOON, null );
                spoonLoggingObject.setContainerObjectId( executionId );
                spoonLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );
                job = new Job( App.getInstance().getRepository(), jobMeta, spoonLoggingObject );
                job.setLogLevel(executionConfiguration.getLogLevel());
                job.shareVariablesWith(jobMeta);
                job.setInteractive(true);
                job.setGatheringMetrics(executionConfiguration.isGatheringMetrics());
                job.setArguments(executionConfiguration.getArgumentStrings());

                job.getExtensionDataMap().putAll(executionConfiguration.getExtensionOptions());

                // If there is an alternative start job entry, pass it to the job
                //
                if ( !Const.isEmpty( executionConfiguration.getStartCopyName() ) ) {
                    JobEntryCopy startJobEntryCopy = jobMeta.findJobEntry( executionConfiguration.getStartCopyName(), executionConfiguration.getStartCopyNr(), false );
                    job.setStartJobEntryCopy( startJobEntryCopy );
                }
                // Set the named parameters
                Map<String, String> paramMap = executionConfiguration.getParams();
                Set<String> keys = paramMap.keySet();
                for (String key : keys) {
                    job.getJobMeta().setParameterValue(key, Const.NVL(paramMap.get(key), ""));
                }
                job.getJobMeta().activateParameters();
                job.start();
                while(!job.isFinished()){
                    Thread.sleep(500);
                }
                errCount = job.getErrors();
            } else if (executionConfiguration.isExecutingRemotely()) {
                carteObjectId = Job.sendToSlaveServer( jobMeta, executionConfiguration, App.getInstance().getRepository(), App.getInstance().getMetaStore() );

                SlaveServer remoteSlaveServer = executionConfiguration.getRemoteServer();
                boolean running = true;
                while(running) {
                    SlaveServerJobStatus jobStatus = remoteSlaveServer.getJobStatus(jobMeta.getName(), carteObjectId, 0);
                    running = jobStatus.isRunning();
                    if(!running)
                        errCount = jobStatus.getResult().getNrErrors();
                    Thread.sleep(500);
                }
            }

        } catch(Exception e) {

        } finally {

        }
    }
    public void stop(){
        if(null!=job){
            job.stopAll();
        }
    }
}