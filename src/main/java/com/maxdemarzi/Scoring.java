package com.maxdemarzi;

import com.maxdemarzi.Schema.Labels;
import com.maxdemarzi.Schema.RelationshipTypes;
import com.maxdemarzi.results.MapResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class Scoring {


    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;


    @Procedure(name = "com.maxdemarzi.scoring_space", mode = Mode.READ)
    @Description("CALL com.maxdemarzi.scoring_space(dnum) - scoring document")
    public Stream<MapResult> scoringSpace(@Name("dnum") String dnum) {
        ArrayList<HashMap<String, Object>> results = new ArrayList<>();

        Node anchor = db.findNode(Labels.Document, "dnum", dnum);

        if (anchor != null) {
            String anchorIdentifier = (String)anchor.getProperty("identifier");

            for (Relationship at : anchor.getRelationships(Direction.BOTH, RelationshipTypes.CITES) ) {
                Node through = at.getOtherNode(anchor);
                String throughIdentifier = (String)through.getProperty("identifier");
                Map<String, Object> atProperties = at.getAllProperties();

                for (Relationship tc : through.getRelationships(Direction.BOTH, RelationshipTypes.CITES)) {
                    Node connected = tc.getOtherNode(through);
                    String connectedIdentifier = (String)connected.getProperty("identifier");
                    Map<String, Object> tcProperties = tc.getAllProperties();
                    String atDirection;
                    String tcDirection;

                    if (anchor.equals(at.getStartNode())) {
                        atDirection = "backward";
                    } else {
                        atDirection = "forwards";
                    }
                    if (through.equals(tc.getStartNode())) {
                        tcDirection= "backward";
                    } else {
                        tcDirection = "forwards";
                    }

                    for(Relationship b : connected.getRelationships(Direction.OUTGOING, RelationshipTypes.ASSIGNED_TO)) {
                        HashMap<String, Object> result = new HashMap<>();
                        result.put("anchor.identifier", anchorIdentifier);
                        result.put("through.identifier", throughIdentifier);
                        result.put("connected.identifier", connectedIdentifier);
                        result.put("at.date_init", atProperties.getOrDefault("date_init", null));
                        result.put("at.date_null", atProperties.getOrDefault("date_null", null));
                        result.put("tc.date_init", tcProperties.getOrDefault("date_init", null));
                        result.put("tc.date_null", tcProperties.getOrDefault("date_null", null));
                        result.put("at_direction", atDirection);
                        result.put("tc_direction", tcDirection);
                        result.put("b.date_init", b.getProperty("date_init", null));
                        result.put("b.date_null", b.getProperty("date_null", null));
                        result.put("group.name", b.getEndNode().getProperty("name", null));
                        results.add(result);
                    }
                }
            }
        }

        return results.stream().map(MapResult::new);
    }

    @Procedure(name = "com.maxdemarzi.scoring_space2", mode = Mode.READ)
    @Description("CALL com.maxdemarzi.scoring_space2(dnum) - scoring document")
    public Stream<MapResult> scoringSpace2(@Name("dnum") String dnum) {
        ArrayList<HashMap<String, Object>> results = new ArrayList<>();

        Node anchor = db.findNode(Labels.Document, "dnum", dnum);

        if (anchor != null) {
            String anchorIdentifier = (String)anchor.getProperty("identifier");
            // out, out
            for (Relationship at : anchor.getRelationships(Direction.OUTGOING, RelationshipTypes.CITES) ) {
                Node through = at.getEndNode();
                String throughIdentifier = (String)through.getProperty("identifier");
                Map<String, Object> atProperties = at.getAllProperties();
                for (Relationship tc : through.getRelationships(Direction.OUTGOING, RelationshipTypes.CITES)) {
                    Node connected = tc.getEndNode();
                    String connectedIdentifier = (String) connected.getProperty("identifier");
                    Map<String, Object> tcProperties = tc.getAllProperties();

                    for (Relationship b : connected.getRelationships(Direction.OUTGOING, RelationshipTypes.ASSIGNED_TO)) {
                        HashMap<String, Object> result = new HashMap<>();
                        result.put("anchor.identifier", anchorIdentifier);
                        result.put("through.identifier", throughIdentifier);
                        result.put("connected.identifier", connectedIdentifier);
                        result.put("at.date_init", atProperties.getOrDefault("date_init", null));
                        result.put("at.date_null", atProperties.getOrDefault("date_null", null));
                        result.put("tc.date_init", tcProperties.getOrDefault("date_init", null));
                        result.put("tc.date_null", tcProperties.getOrDefault("date_null", null));
                        result.put("at_direction", "forwards");
                        result.put("tc_direction", "forwards");
                        result.put("b.date_init", b.getProperty("date_init", null));
                        result.put("b.date_null", b.getProperty("date_null", null));
                        result.put("group.name", b.getEndNode().getProperty("name", null));
                        results.add(result);
                    }
                }
            }

            // out, in
            for (Relationship at : anchor.getRelationships(Direction.OUTGOING, RelationshipTypes.CITES) ) {
                Node through = at.getEndNode();
                String throughIdentifier = (String)through.getProperty("identifier");
                Map<String, Object> atProperties = at.getAllProperties();
                for (Relationship tc : through.getRelationships(Direction.INCOMING, RelationshipTypes.CITES)) {
                    Node connected = tc.getStartNode();
                    String connectedIdentifier = (String) connected.getProperty("identifier");
                    Map<String, Object> tcProperties = tc.getAllProperties();

                    for (Relationship b : connected.getRelationships(Direction.OUTGOING, RelationshipTypes.ASSIGNED_TO)) {
                        HashMap<String, Object> result = new HashMap<>();
                        result.put("anchor.identifier", anchorIdentifier);
                        result.put("through.identifier", throughIdentifier);
                        result.put("connected.identifier", connectedIdentifier);
                        result.put("at.date_init", atProperties.getOrDefault("date_init", null));
                        result.put("at.date_null", atProperties.getOrDefault("date_null", null));
                        result.put("tc.date_init", tcProperties.getOrDefault("date_init", null));
                        result.put("tc.date_null", tcProperties.getOrDefault("date_null", null));
                        result.put("at_direction", "forwards");
                        result.put("tc_direction", "backwards");
                        result.put("b.date_init", b.getProperty("date_init", null));
                        result.put("b.date_null", b.getProperty("date_null", null));
                        result.put("group.name", b.getEndNode().getProperty("name", null));
                        results.add(result);
                    }
                }
            }

            // in, out
            for (Relationship at : anchor.getRelationships(Direction.INCOMING, RelationshipTypes.CITES) ) {
                Node through = at.getStartNode();
                String throughIdentifier = (String)through.getProperty("identifier");
                Map<String, Object> atProperties = at.getAllProperties();
                for (Relationship tc : through.getRelationships(Direction.OUTGOING, RelationshipTypes.CITES)) {
                    Node connected = tc.getEndNode();
                    String connectedIdentifier = (String) connected.getProperty("identifier");
                    Map<String, Object> tcProperties = tc.getAllProperties();

                    for (Relationship b : connected.getRelationships(Direction.INCOMING, RelationshipTypes.ASSIGNED_TO)) {
                        HashMap<String, Object> result = new HashMap<>();
                        result.put("anchor.identifier", anchorIdentifier);
                        result.put("through.identifier", throughIdentifier);
                        result.put("connected.identifier", connectedIdentifier);
                        result.put("at.date_init", atProperties.getOrDefault("date_init", null));
                        result.put("at.date_null", atProperties.getOrDefault("date_null", null));
                        result.put("tc.date_init", tcProperties.getOrDefault("date_init", null));
                        result.put("tc.date_null", tcProperties.getOrDefault("date_null", null));
                        result.put("at_direction", "backwards");
                        result.put("tc_direction", "forwards");
                        result.put("b.date_init", b.getProperty("date_init", null));
                        result.put("b.date_null", b.getProperty("date_null", null));
                        result.put("group.name", b.getEndNode().getProperty("name", null));
                        results.add(result);
                    }
                }
            }

            // in, in
            for (Relationship at : anchor.getRelationships(Direction.INCOMING, RelationshipTypes.CITES) ) {
                Node through = at.getStartNode();
                String throughIdentifier = (String)through.getProperty("identifier");
                Map<String, Object> atProperties = at.getAllProperties();
                for (Relationship tc : through.getRelationships(Direction.INCOMING, RelationshipTypes.CITES)) {
                    Node connected = tc.getStartNode();
                    String connectedIdentifier = (String) connected.getProperty("identifier");
                    Map<String, Object> tcProperties = tc.getAllProperties();

                    for (Relationship b : connected.getRelationships(Direction.INCOMING, RelationshipTypes.ASSIGNED_TO)) {
                        HashMap<String, Object> result = new HashMap<>();
                        result.put("anchor.identifier", anchorIdentifier);
                        result.put("through.identifier", throughIdentifier);
                        result.put("connected.identifier", connectedIdentifier);
                        result.put("at.date_init", atProperties.getOrDefault("date_init", null));
                        result.put("at.date_null", atProperties.getOrDefault("date_null", null));
                        result.put("tc.date_init", tcProperties.getOrDefault("date_init", null));
                        result.put("tc.date_null", tcProperties.getOrDefault("date_null", null));
                        result.put("at_direction", "backwards");
                        result.put("tc_direction", "backwards");
                        result.put("b.date_init", b.getProperty("date_init", null));
                        result.put("b.date_null", b.getProperty("date_null", null));
                        result.put("group.name", b.getEndNode().getProperty("name", null));
                        results.add(result);
                    }
                }
            }

        }

        return results.stream().map(MapResult::new);
    }
}
