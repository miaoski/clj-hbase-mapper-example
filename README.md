clj-hbase-mapper-example
========================

A Clojure example to put records into HBase using Hadoop mappers.

# Description

Having used Kerberos to authenticate Hadoop's map-reduce jobs, 
User.isSecurityEnabled() is true, and 
User.getCurrent().obtainAuthTokenForJob(job.getConfiguration, job)
must be called before submitting a job.

Here is an example of setting such an authentication token in Clojure.


# Notes

Use lein uberjar to build standalone JAR.

## License
BSD License.
