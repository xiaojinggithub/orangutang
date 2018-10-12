package com.orangutang.orangutang.service.other.kettle;


import org.apache.commons.dbcp2.BasicDataSource;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.sql.DataSource;
import java.util.ArrayList;


public class App implements ApplicationContextAware {

    private static App app;
    public static KettleDatabaseRepositoryMeta meta;

    private LogChannelInterface log;
    private TransExecutionConfiguration transExecutionConfiguration;
    private TransExecutionConfiguration transPreviewExecutionConfiguration;
    private TransExecutionConfiguration transDebugExecutionConfiguration;
    private JobExecutionConfiguration jobExecutionConfiguration;

    private App() {
        transExecutionConfiguration = new TransExecutionConfiguration();
        transExecutionConfiguration.setGatheringMetrics( true );
        transPreviewExecutionConfiguration = new TransExecutionConfiguration();
        transPreviewExecutionConfiguration.setGatheringMetrics( true );
        transDebugExecutionConfiguration = new TransExecutionConfiguration();
        transDebugExecutionConfiguration.setGatheringMetrics( true );

        jobExecutionConfiguration = new JobExecutionConfiguration();

        variables = new RowMetaAndData( new RowMeta() );
    }


    public static App getInstance() {
        if (app == null) {
            app = new App();
        }
        return app;
    }

    private Repository repository;

    public Repository getRepository() {
        return repository;
    }

    private Repository defaultRepository;


    public Repository getDefaultRepository() {
        return this.defaultRepository;
    }

    public void selectRepository(Repository repo) {
        if(repository != null) {
            repository.disconnect();
        }
        repository = repo;
    }

    private DelegatingMetaStore metaStore;

    public DelegatingMetaStore getMetaStore() {
        return metaStore;
    }

    public LogChannelInterface getLog() {
        return log;
    }

    private RowMetaAndData variables = null;
    private ArrayList<String> arguments = new ArrayList<String>();

    public String[] getArguments() {
        return arguments.toArray(new String[arguments.size()]);
    }

    public JobExecutionConfiguration getJobExecutionConfiguration() {
        return jobExecutionConfiguration;
    }

    public TransExecutionConfiguration getTransDebugExecutionConfiguration() {
        return transDebugExecutionConfiguration;
    }

    public TransExecutionConfiguration getTransPreviewExecutionConfiguration() {
        return transPreviewExecutionConfiguration;
    }

    public TransExecutionConfiguration getTransExecutionConfiguration() {
        return transExecutionConfiguration;
    }

    public RowMetaAndData getVariables() {
        return variables;
    }

    @Override
    public void  setApplicationContext(ApplicationContext context) throws BeansException {
        KettleDatabaseRepository repository = new KettleDatabaseRepository();
        try {
            BasicDataSource dataSource = (BasicDataSource) context.getBean(DataSource.class);
            DatabaseMeta dbMeta = new DatabaseMeta();

            String url = dataSource.getUrl();
            String hostname = url.substring(url.indexOf("//") + 2, url.lastIndexOf(":"));
            String port = url.substring(url.lastIndexOf(":") + 1, url.lastIndexOf("/"));
            String dbName = url.substring(url.lastIndexOf("/") + 1);

            dbMeta.setName("192.168.1.201_kettle");
            dbMeta.setDBName(dbName);
            dbMeta.setDatabaseType("MYSQL");
            dbMeta.setAccessType(0);
            dbMeta.setHostname(hostname);
            dbMeta.setServername(hostname);
            dbMeta.setDBPort(port);
            dbMeta.setUsername(dataSource.getUsername());
            dbMeta.setPassword(dataSource.getPassword());
            ObjectId objectId = new LongObjectId(100);
            dbMeta.setObjectId(objectId);
            dbMeta.setShared(true);
            dbMeta.addExtraOption(dbMeta.getPluginId(), "characterEncoding", "utf8");
            dbMeta.addExtraOption(dbMeta.getPluginId(), "useUnicode", "true");
            dbMeta.addExtraOption(dbMeta.getPluginId(), "autoReconnect", "true");
            meta = new KettleDatabaseRepositoryMeta();
            meta.setName("kettle");
            meta.setId("KettleDatabaseRepository");
            meta.setConnection(dbMeta);
            meta.setDescription("kettle");

            repository.init(meta);
            repository.connect("admin", "admin");
            this.repository = repository;
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}