package com.zendesk.maxwell;

import java.util.*;

import joptsimple.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.util.AbstractConfig;

public class MaxwellConfig extends AbstractConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

	public MaxwellMysqlConfig replicationMysql;

	public MaxwellMysqlConfig maxwellMysql;
	public MaxwellFilter filter;

	public String databaseName;

	public String  includeDatabases, excludeDatabases, includeTables, excludeTables, excludeColumns, blacklistDatabases, blacklistTables;

	public final Properties kafkaProperties;
	public String kafkaTopic;
	public String kafkaKeyFormat;
	public String producerType;
	public String kafkaPartitionHash;
	public String kafkaPartitionKey;
	public String bootstrapperType;

    public String kinesisAccessKeyId;
    public String kinesisSecretKey;
    public int kinesisMaxBufferedTime;
    public int kinesisMaxConnections;
    public int kinesisRequestTimeout;
    public int kinesisConnectTimeout;
    public String kinesisRegion;
    public String kinesisStreamName;
    
    public String consulUrl;
    public String consulKey;
    public String consulLockSession;

	public String outputFile;
	public String log_level;

	public BinlogPosition initPosition;
	public boolean replayMode;

	public MaxwellConfig() { // argv is only null in tests
		this.kafkaProperties = new Properties();
		this.replayMode = false;
		this.replicationMysql = new MaxwellMysqlConfig();
		this.maxwellMysql = new MaxwellMysqlConfig();
	}

	public MaxwellConfig(String argv[]) {
		this();
		this.parse(argv);
		this.validate();
	}

	protected OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config file" ).withRequiredArg();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR" ).withRequiredArg();

		parser.accepts( "__separator_1" );

		parser.accepts( "host", "mysql host with write access to maxwell database" ).withRequiredArg();
		parser.accepts( "port", "port for host" ).withRequiredArg();
		parser.accepts( "user", "username for host" ).withRequiredArg();
		parser.accepts( "password", "password for host" ).withOptionalArg();
		parser.accepts( "jdbc_options", "additional jdbc connection options" ).withOptionalArg();

		parser.accepts( "__separator_2" );

		parser.accepts( "replication_host", "mysql host to replicate from (if using separate schema and replication servers)" ).withRequiredArg();
		parser.accepts( "replication_user", "username for replication_host" ).withRequiredArg();
		parser.accepts( "replication_password", "password for replication_host" ).withOptionalArg();
		parser.accepts( "replication_port", "port for replicattion_host" ).withRequiredArg();

		parser.accepts( "__separator_3" );

		parser.accepts( "producer", "producer type: stdout|file|kafka|kinesis" ).withRequiredArg();
		parser.accepts( "output_file", "output file for 'file' producer" ).withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_partition_by", "database|table|primary_key, kafka producer assigns partition by hashing the specified parameter").withRequiredArg();
		parser.accepts( "kafka_partition_hash", "default|murmur3, hash function for partitioning").withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell").withOptionalArg();
		parser.accepts( "kafka_key_format", "how to format the kafka key; array|hash").withOptionalArg();
        parser.accepts( "kinesis_access_key_id", "optionally provide kinesis access key id").withOptionalArg();
        parser.accepts( "kinesis_secret_key", "optionally provide kinesis secret key").withOptionalArg();
        parser.accepts( "kinesis_max_buffered_time", "optionally provide kinesis max buffered time").withOptionalArg();
        parser.accepts( "kinesis_max_connections", "optionally provide kinesis max connections").withOptionalArg();
        parser.accepts( "kinesis_request_timeout", "optionally provide kinesis request timeout").withOptionalArg();
        parser.accepts( "kinesis_connect_timeout", "optionally provide kinesis connect timeout").withOptionalArg();
        parser.accepts( "kinesis_region", "optionally provide kinesis region").withOptionalArg();
        parser.accepts( "kinesis_stream_name", "optionally provide kinesis stream name").withOptionalArg();
        
        parser.accepts( "consul_url", "URL for Consul host" ).withOptionalArg();
        parser.accepts( "consul_key", "Key for Consul lock" ).withOptionalArg();
        parser.accepts( "consul_lock_session", "Consul session name" ).withOptionalArg();

		parser.accepts( "__separator_4" );

		parser.accepts( "bootstrapper", "bootstrapper type: async|sync|none. default: async" ).withRequiredArg();
		parser.accepts( "bootstrapper_fetch_size", "(deprecated)" ).withRequiredArg();

		parser.accepts( "__separator_5" );

		parser.accepts( "schema_database", "database name for maxwell state (schema and binlog position)").withRequiredArg();
		parser.accepts( "max_schemas", "deprecated.").withOptionalArg();
		parser.accepts( "init_position", "initial binlog position, given as BINLOG_FILE:POSITION").withRequiredArg();
		parser.accepts( "replay", "replay mode, don't store any information to the server");

		parser.accepts( "__separator_6" );

		parser.accepts( "include_dbs", "include these databases, formatted as include_dbs=db1,db2").withOptionalArg();
		parser.accepts( "exclude_dbs", "exclude these databases, formatted as exclude_dbs=db1,db2").withOptionalArg();
		parser.accepts( "include_tables", "include these tables, formatted as include_tables=db1,db2").withOptionalArg();
		parser.accepts( "exclude_tables", "exclude these tables, formatted as exclude_tables=tb1,tb2").withOptionalArg();
		parser.accepts( "exclude_columns", "exclude these columns, formatted as exclude_columns=col1,col2" ).withOptionalArg();
		parser.accepts( "blacklist_dbs", "ignore data AND schema changes to these databases, formatted as blacklist_dbs=db1,db2. See the docs for details before setting this!").withOptionalArg();
		parser.accepts( "blacklist_tables", "ignore data AND schema changes to these tables, formatted as blacklist_tables=tb1,tb2. See the docs for details before setting this!").withOptionalArg();

		parser.accepts( "__separator_7" );

		parser.accepts( "help", "display help").forHelp();

		BuiltinHelpFormatter helpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				return output.replaceAll("--__separator_.*", "");
			}
		};

		parser.formatHelpWith(helpFormatter);
		return parser;
	}

	private String parseLogLevel(String level) {
		level = level.toLowerCase();
		if ( !( level.equals("debug") || level.equals("info") || level.equals("warn") || level.equals("error")))
			usageForOptions("unknown log level: " + level, "--log_level");
		return level;
	}

	private void parse(String [] argv) {
		OptionSet options = buildOptionParser().parse(argv);

		if ( options.has("config") ) {
			parseFile((String) options.valueOf("config"), true);
		} else {
			parseFile(DEFAULT_CONFIG_FILE, false);
		}

		if ( options.has("help") )
			usage("Help for Maxwell:");

		if ( options.has("log_level")) {
			this.log_level = parseLogLevel((String) options.valueOf("log_level"));
		}

		this.maxwellMysql.parseOptions("", options);

		this.replicationMysql.parseOptions("replication_", options);

		if ( options.has("schema_database")) {
			this.databaseName = (String) options.valueOf("schema_database");
		}

		if ( options.has("producer"))
			this.producerType = (String) options.valueOf("producer");
		if ( options.has("bootstrapper"))
			this.bootstrapperType = (String) options.valueOf("bootstrapper");

		if ( options.has("kafka.bootstrap.servers"))
			this.kafkaProperties.setProperty("bootstrap.servers", (String) options.valueOf("kafka.bootstrap.servers"));

		if ( options.has("kafka_topic"))
			this.kafkaTopic = (String) options.valueOf("kafka_topic");

		if ( options.has("kafka_key_format"))
			this.kafkaKeyFormat = (String) options.valueOf("kafka_key_format");

		if ( options.has("kafka_partition_by"))
			this.kafkaPartitionKey = (String) options.valueOf("kafka_partition_by");

		if ( options.has("kafka_partition_hash"))
			this.kafkaPartitionHash = (String) options.valueOf("kafka_partition_hash");

        if ( options.has("kinesis_access_key_id"))
            this.kinesisAccessKeyId = (String) options.valueOf("kinesis_access_key_id");

        if ( options.has("kinesis_secret_key"))
            this.kinesisSecretKey = (String) options.valueOf("kinesis_secret_key");

        if ( options.has("kinesis_max_buffered_time"))
            this.kinesisMaxBufferedTime = (int) options.valueOf("kinesis_max_buffered_time");

        if ( options.has("kinesis_max_connections"))
            this.kinesisMaxConnections = (int) options.valueOf("kinesis_max_connections");

        if ( options.has("kinesis_request_timeout"))
            this.kinesisRequestTimeout = (int) options.valueOf("kinesis_request_timeout");

        if ( options.has("kinesis_connect_timeout"))
            this.kinesisConnectTimeout = (int) options.valueOf("kinesis_connect_timeout");

        if ( options.has("kinesis_region"))
            this.kinesisRegion = (String) options.valueOf("kinesis_region");

        if ( options.has("kinesis_stream_name"))
            this.kinesisStreamName = (String) options.valueOf("kinesis_stream_name");
        
        if ( options.has("consul_url"))
        	this.consulUrl = (String) options.valueOf("consul_url");
        
        if ( options.has("consul_key"))
        	this.consulKey = (String) options.valueOf("consul_key");
        
        if ( options.has("consul_lock_session"))
        	this.consulLockSession = (String) options.valueOf("consul_lock_session");

		if ( options.has("output_file"))
			this.outputFile = (String) options.valueOf("output_file");

		if ( options.has("init_position")) {
			String initPosition = (String) options.valueOf("init_position");
			String[] initPositionSplit = initPosition.split(":");

			if ( initPositionSplit.length != 2 )
				usageForOptions("Invalid init_position: " + initPosition, "--init_position");

			Long pos = 0L;
			try {
				pos = Long.valueOf(initPositionSplit[1]);
			} catch ( NumberFormatException e ) {
				usageForOptions("Invalid init_position: " + initPosition, "--init_position");
			}

			this.initPosition = new BinlogPosition(pos, initPositionSplit[0]);
		}

		if ( options.has("replay")) {
			this.replayMode = true;
		}

		if ( options.has("include_dbs"))
			this.includeDatabases = (String) options.valueOf("include_dbs");

		if ( options.has("exclude_dbs"))
			this.excludeDatabases = (String) options.valueOf("exclude_dbs");

		if ( options.has("include_tables"))
			this.includeTables = (String) options.valueOf("include_tables");

		if ( options.has("exclude_tables"))
			this.excludeTables = (String) options.valueOf("exclude_tables");

		if ( options.has("blacklist_dbs"))
			this.blacklistDatabases = (String) options.valueOf("blacklist_dbs");

		if ( options.has("blacklist_tables"))
			this.blacklistTables = (String) options.valueOf("blacklist_tables");

		if ( options.has("exclude_columns") ) {
			this.excludeColumns = (String) options.valueOf("exclude_columns");
		}
	}

	private void parseFile(String filename, Boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			p = new Properties();

		this.maxwellMysql.host = p.getProperty("host");
		this.maxwellMysql.password = p.getProperty("password");
		this.maxwellMysql.user     = p.getProperty("user", "maxwell");
		this.maxwellMysql.port = Integer.valueOf(p.getProperty("port", "3306"));
		this.maxwellMysql.parseJDBCOptions(p.getProperty("jdbc_options"));

		this.replicationMysql.host = p.getProperty("replication_host");
		this.replicationMysql.password = p.getProperty("replication_password");
		this.replicationMysql.user      = p.getProperty("replication_user");
		this.replicationMysql.port = Integer.valueOf(p.getProperty("replication_port", "3306"));
		this.replicationMysql.parseJDBCOptions(p.getProperty("jdbc_options"));

		this.databaseName = p.getProperty("schema_database", "maxwell");

		this.producerType    = p.getProperty("producer", "stdout");
		this.bootstrapperType = p.getProperty("bootstrapper", "async");

		this.outputFile      = p.getProperty("output_file");
		this.kafkaTopic      = p.getProperty("kafka_topic");
		this.kafkaPartitionHash = p.getProperty("kafka_partition_hash", "default");
		this.kafkaPartitionKey = p.getProperty("kafka_partition_by", "database");
		this.kafkaKeyFormat = p.getProperty("kafka_key_format", "hash");
		this.includeDatabases = p.getProperty("include_dbs");
		this.excludeDatabases = p.getProperty("exclude_dbs");
		this.includeTables = p.getProperty("include_tables");
		this.excludeTables = p.getProperty("exclude_tables");
		this.excludeColumns = p.getProperty("exclude_columns");
		this.blacklistDatabases = p.getProperty("blacklist_dbs");
		this.blacklistTables = p.getProperty("blacklist_tables");

        this.kinesisAccessKeyId = p.getProperty("kinesis_access_key_id");
        this.kinesisSecretKey = p.getProperty("kinesis_secret_key");
        this.kinesisMaxBufferedTime = Integer.valueOf(p.getProperty("kinesis_max_buffered_time", "0"));
        this.kinesisMaxConnections = Integer.valueOf(p.getProperty("kinesis_max_connections", "0"));
        this.kinesisRequestTimeout = Integer.valueOf(p.getProperty("kinesis_request_timeout", "0"));
        this.kinesisConnectTimeout = Integer.valueOf(p.getProperty("kinesis_connect_timeout", "0"));
        this.kinesisRegion = p.getProperty("kinesis_region");
        this.kinesisStreamName = p.getProperty("kinesis_stream_name");
        
        this.consulUrl = p.getProperty("consul_url");
        this.consulKey = p.getProperty("consul_key");
        this.consulLockSession = p.getProperty("consul_lock_session");
        

		if ( p.containsKey("log_level") )
			this.log_level = parseLogLevel(p.getProperty("log_level"));

		for ( Enumeration<Object> e = p.keys(); e.hasMoreElements(); ) {
			String k = (String) e.nextElement();
			if ( k.startsWith("kafka.")) {
				this.kafkaProperties.setProperty(k.replace("kafka.", ""), p.getProperty(k));
			}
		}

	}

	private void validate() {
		if ( this.producerType.equals("kafka") ) {
			if ( !this.kafkaProperties.containsKey("bootstrap.servers") ) {
				usageForOptions("You must specify kafka.bootstrap.servers for the kafka producer!", "kafka");
			}

			if ( this.kafkaPartitionHash == null ) {
				this.kafkaPartitionHash = "default";
			} else if ( !this.kafkaPartitionHash.equals("default")
					&& !this.kafkaPartitionHash.equals("murmur3") ) {
				usageForOptions("please specify --kafka_partition_hash=default|murmur3", "kafka_partition_hash");
			}

			if ( this.kafkaPartitionKey == null ) {
				this.kafkaPartitionKey = "database";
			} else if ( !this.kafkaPartitionKey.equals("database")
					&& !this.kafkaPartitionKey.equals("table")
					&& !this.kafkaPartitionKey.equals("primary_key") ) {
				usageForOptions("please specify --kafka_partition_by=database|table|primary_key", "kafka_partition_by");
			}


			if ( !this.kafkaKeyFormat.equals("hash") && !this.kafkaKeyFormat.equals("array") )
				usageForOptions("invalid kafka_key_format: " + this.kafkaKeyFormat, "kafka_key_format");

		} else if ( this.producerType.equals("file")
				&& this.outputFile == null) {
			usageForOptions("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		} else if ( this.producerType.equals("kinesis") ) {
            if ( this.kinesisAccessKeyId == null ) {
                usageForOptions("You must provide aws kinesis access key id for using kinesis as output sink!", "--kinesis");
            }
            if ( this.kinesisSecretKey == null ) {
                usageForOptions("You must provide aws kinesis secret key for using kinesis as output sink!", "--kinesis");
            }
            if ( this.kinesisMaxBufferedTime == 0 ) {
                usageForOptions("You must provide aws kinesis max buffered time for using kinesis as output sink!", "--kinesis");
            }
            if ( this.kinesisMaxConnections == 0 ) {
                usageForOptions("You must provide aws kinesis max connections for using kinesis as output sink!", "--kinesis");
            }
            if ( this.kinesisRequestTimeout == 0 ) {
                usageForOptions("You must provide aws kinesis request timeout for using kinesis as output sink!", "--kinesis");
            }
            if ( this.kinesisConnectTimeout == 0 ) {
                usageForOptions("You must provide aws kinesis connect timeout for using kinesis as output sink!", "--kinesis");
            }
            if ( this.kinesisRegion == null ) {
                usageForOptions("You must provide aws kinesis region for using kinesis as output sink!", "--kinesis");
            }
        }

		if ( !this.bootstrapperType.equals("async")
				&& !this.bootstrapperType.equals("sync")
				&& !this.bootstrapperType.equals("none") ) {
			usageForOptions("please specify --bootstrapper=async|sync|none", "--bootstrapper");
		}

		if ( this.maxwellMysql.host == null ) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			this.maxwellMysql.host = "localhost";
		}

		if ( this.replicationMysql.host != null && !this.bootstrapperType.equals("none") ) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			this.bootstrapperType = "none";
		}

		if ( this.replicationMysql.host == null
				|| this.replicationMysql.user == null ) {

			if (this.replicationMysql.host != null
					|| this.replicationMysql.user != null
					|| this.replicationMysql.password != null) {
				usageForOptions("Please specify all of: replication_host, replication_user, replication_password", "--replication");
			}

			this.replicationMysql = new MaxwellMysqlConfig(this.maxwellMysql.host,
									this.maxwellMysql.port,
									this.maxwellMysql.user,
									this.maxwellMysql.password);

			this.replicationMysql.jdbcOptions = this.maxwellMysql.jdbcOptions;
		}

		if ( this.databaseName == null) {
			this.databaseName = "maxwell";
		}

		try {
			this.filter = new MaxwellFilter(
					includeDatabases,
					excludeDatabases,
					includeTables,
					excludeTables,
					blacklistDatabases,
					blacklistTables,
					excludeColumns
			);
		} catch (MaxwellInvalidFilterException e) {
			usage("Invalid filter options: " + e.getLocalizedMessage());
		}
	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}
}
