# related_documents
Related Documents Stored Procedure

This project requires Neo4j 3.3.x or higher

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/procedures-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/procedures-1.0-SNAPSHOT.jar neo4j-enterprise-3.3.0/plugins/.
    
Edit your Neo4j/conf/neo4j.conf file by adding this line:

    dbms.security.procedures.unrestricted=com.maxdemarzi.*    

Restart your Neo4j Server.


Try it:

    CALL com.maxdemarzi.scoring_space("123") yield value return value;    
    