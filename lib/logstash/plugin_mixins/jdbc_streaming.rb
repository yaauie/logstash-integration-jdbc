# encoding: utf-8
require "logstash/config/mixin"
require_relative "wrapped_driver"

# Tentative of abstracting JDBC logic to a mixin
# for potential reuse in other plugins (input/output)
module LogStash module PluginMixins module JdbcStreaming

  # This method is called when someone includes this module
  def self.included(base)
    # Add these methods to the 'base' given.
    base.extend(self)
    base.setup_jdbc_config
  end

  public
  def setup_jdbc_config
    # JDBC driver library path to third party driver library.
    config :jdbc_driver_library, :validate => :path

    # JDBC driver class to load, for example "oracle.jdbc.OracleDriver" or "org.apache.derby.jdbc.ClientDriver"
    config :jdbc_driver_class, :validate => :string, :required => true

    # JDBC connection string
    config :jdbc_connection_string, :validate => :string, :required => true

    # JDBC user
    config :jdbc_user, :validate => :string

    # JDBC password
    config :jdbc_password, :validate => :password

    # Connection pool configuration.
    # Validate connection before use.
    config :jdbc_validate_connection, :validate => :boolean, :default => false

    # Connection pool configuration.
    # How often to validate a connection (in seconds)
    config :jdbc_validation_timeout, :validate => :number, :default => 3600
  end

  private

  def load_drivers
    return if @jdbc_driver_library.nil? || @jdbc_driver_library.empty?
    driver_jars = @jdbc_driver_library.split(",")

    # Needed for JDK 11 as the DriverManager has a different ClassLoader than Logstash
    urls = java.net.URL[driver_jars.length].new
    driver_jars.each_with_index do |driver, idx|
        urls[idx] = java.io.File.new(driver).toURI().toURL()
      end
      ucl = java.net.URLClassLoader.new_instance(urls)
      begin
        klass = java.lang.Class.forName(@jdbc_driver_class.to_java(:string), true, ucl);
      rescue Java::JavaLang::ClassNotFoundException => e
        raise LogStash::Error, "Unable to find driver class via URLClassLoader in given driver jars: #{@jdbc_driver_class}"
      end
      begin
        driver = klass.getConstructor().newInstance();
        java.sql.DriverManager.register_driver(WrappedDriver.new(driver.to_java(java.sql.Driver)).to_java(java.sql.Driver))
      rescue Java::JavaSql::SQLException => e
        raise LogStash::Error, "Unable to register driver with java.sql.DriverManager using WrappedDriver: #{@jdbc_driver_class}"
      end

  end

  public
  def prepare_jdbc_connection
    require "sequel"
    require "sequel/adapters/jdbc"
    require "java"

    load_drivers

    Sequel::JDBC.load_driver(@jdbc_driver_class)
    @database = Sequel.connect(@jdbc_connection_string, :user=> @jdbc_user, :password=>  @jdbc_password.nil? ? nil : @jdbc_password.value)
    if @jdbc_validate_connection
      @database.extension(:connection_validator)
      @database.pool.connection_validation_timeout = @jdbc_validation_timeout
    end
    begin
      @database.test_connection
    rescue Sequel::DatabaseConnectionError => e
      #TODO return false and let the plugin raise a LogStash::ConfigurationError
      raise e
    end
  end # def prepare_jdbc_connection
end end end
