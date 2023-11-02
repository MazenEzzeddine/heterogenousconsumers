import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Cluster;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class BinPackPartitionAssignor extends AbstractAssignor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinPackPartitionAssignor.class);
    public BinPackPartitionAssignor() {
    }
    static final String TOPIC_PARTITIONS_KEY_NAME = "previous_assignment";
    static final String TOPIC_KEY_NAME = "topic";
    static final String PARTITIONS_KEY_NAME = "partitions";
    static final String MAX_CONSUMPTION_RATE = "maxConsumptionRate";
    static final String THE_Name = "the_name";

    private static final String GENERATION_KEY_NAME = "generation";
    static final Schema TOPIC_ASSIGNMENT = new Schema(
            new Field(TOPIC_KEY_NAME, Type.STRING),
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32))
    );
    static final Schema STICKY_ASSIGNOR_USER_DATA_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT)));
    private static final Schema STICKY_ASSIGNOR_USER_DATA_V1 = new Schema(
           // new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT)),
            new Field(GENERATION_KEY_NAME, Type.INT32),
            new Field(MAX_CONSUMPTION_RATE, Type.FLOAT64),
            new Field(THE_Name, Type.STRING));

    private int generation = DEFAULT_GENERATION; // consumer group generation

    private static Map<String, Double> memberToRate = null;
    private static Map<String, String> memberToName = null;



    @Override
    protected MemberData memberData(Subscription subscription) {
        ByteBuffer userData = subscription.userData();
        if (userData == null || !userData.hasRemaining()) {
            return new MemberData(
                    0.0d, System.getenv("THENAME"), Optional.empty());
        }
        return deserializeTopicPartitionAssignment(userData);
    }

    private static MemberData deserializeTopicPartitionAssignment(ByteBuffer buffer) {
        Struct struct;
        ByteBuffer copy = buffer.duplicate();
        try {
            struct = STICKY_ASSIGNOR_USER_DATA_V1.read(buffer);
        } catch (Exception e1) {
            try {
                // fall back to older schema
                struct = STICKY_ASSIGNOR_USER_DATA_V0.read(copy);
            } catch (Exception e2) {
                // ignore the consumer's previous assignment if it cannot be parsed
                return new MemberData(0.0d, "empty",  Optional.of(DEFAULT_GENERATION));
            }
        }

        Optional<Integer> generation = struct.hasField(GENERATION_KEY_NAME) ?
                Optional.of(struct.getInt(GENERATION_KEY_NAME)) : Optional.empty();
        Double maxRate = struct.hasField(MAX_CONSUMPTION_RATE) ? struct.getDouble(MAX_CONSUMPTION_RATE) : 0.0;
        String thename = struct.hasField(THE_Name) ? struct.getString(THE_Name) : "empty";

        return new MemberData( maxRate, thename, generation);
    }


    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {

        return serializeTopicPartitionAssignment(new MemberData(
                ConsumerThread.maxConsumptionRatePerConsumer1, System.getenv("THENAME"), Optional.of(generation)));
    }
    // visible for testing
    static ByteBuffer serializeTopicPartitionAssignment(MemberData memberData) {
        Struct struct = new Struct(STICKY_ASSIGNOR_USER_DATA_V1);

        if (memberData.generation.isPresent())
            struct.set(GENERATION_KEY_NAME, memberData.generation.get());
        struct.set(MAX_CONSUMPTION_RATE, memberData.maxConsumptionRate);
        struct.set(THE_Name, memberData.name);


        ByteBuffer buffer = ByteBuffer.allocate(STICKY_ASSIGNOR_USER_DATA_V1.sizeOf(struct));
        STICKY_ASSIGNOR_USER_DATA_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        // TODO
        // if there is something to that is returned and to be saved across generations
        this.generation = metadata.generationId();
        LOGGER.info(" Received the assignment and my partitions are:");

        for (TopicPartition tp : assignment.partitions())
            LOGGER.info("partition : {} {}", tp.toString(), tp.partition());
    }

    @Override
    public String name() {
        return "Singleton Assignor";
    }

    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription subscriptions) {
        memberToRate = new HashMap<>();
        memberToName = new HashMap<>();
        final Set<String> allSubscribedTopics = new HashSet<>();
        final Map<String, List<String>> topicSubscriptions = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry :
                subscriptions.groupSubscription().entrySet()) {
            printPreviousAssignments(subscriptionEntry.getKey(), subscriptionEntry.getValue());
            List<String> topics = subscriptionEntry.getValue().topics();
            //LOGGER.info("maximum consumption rate is {}", );
            allSubscribedTopics.addAll(topics);
            topicSubscriptions.put(subscriptionEntry.getKey(), topics);
        }
        Map<String, List<TopicPartition>> rawAssignments =
                assign( topicSubscriptions);

        // this class has maintains no user data, so just wrap the results
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet()) {
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        }
        return new GroupAssignment(assignments);
    }


    void printPreviousAssignments(String memberid, Subscription sub) {
        MemberData md = memberData(sub);
        memberToRate.put(memberid, md.maxConsumptionRate);
        memberToName.put(memberid, md.name);
        LOGGER.info("MaxConsumptionRate {} for {}", memberid, md.maxConsumptionRate);
        LOGGER.info("name of host is {} memberid {}", memberid, md.name);

    }


    //for each consumer returns the list of topic partitions assigned to it.
    static Map<String, List<TopicPartition>> assign(
            Map<String, List<String>> subscriptions) {
        // each memmber/consumer to its propsective assignment

        final Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            LOGGER.info(" hey {}", memberId);
            assignment.put(memberId, new ArrayList<>());
        }
        List<String> consumers = new ArrayList<>(subscriptions.keySet());
        assignController(
                assignment,
                //topic
                consumers);
        return assignment;
    }




    private static void assignController(
            final Map<String, List<TopicPartition>> assignment,
        /*    final String topic*/
            final List<String> consumers) {
        LOGGER.info("Inside  assignController");
        if (consumers.isEmpty()) {
            LOGGER.info("looks like they are empty");
            return;
        }
        for(String c: consumers) {
            LOGGER.info("We have the following consumers  out of Kafka {}", c);
        }
        for (String cons: consumers) {
            LOGGER.info("member id {} is equivalent to host id {} :", cons,  memberToName.get(cons));
        }
        List<Consumer> asscons = callForAssignment();
        for (String co : consumers) {
            Consumer controllerconsumer = null;
            for(Consumer contcons : asscons) {
                LOGGER.info(contcons.getId());
                LOGGER.info(memberToName.get(co));
                if (contcons.getId().equals(memberToName.get(co))) {
                    controllerconsumer = contcons;
                    break;
                }
            }
            LOGGER.info("consumer out of controller  {}", controllerconsumer.getId());
            List<TopicPartition> listtp = new ArrayList<>();
            LOGGER.info("Assigning for kafka consumer {}", co);
            for (Partition p : controllerconsumer.getAssignedPartitionsList()) {
                TopicPartition tp = new TopicPartition("testtopic1", p.getId());
                listtp.add(tp);
                LOGGER.info("Added partition {} to  consumer {}", tp.partition(),
                        controllerconsumer.getId());
            }
            assignment.put(co, listtp);
            for (TopicPartition tp : listtp) {
                LOGGER.info("Assigned partition {} to consumer {}", tp.partition(), co);
            }
        }
    }



    private static List<Consumer> callForAssignment() {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("assignmentservice", 5002)
                .usePlaintext()
                .build();
        AssignmentServiceGrpc.AssignmentServiceBlockingStub assignmentServiceBlockingStub = AssignmentServiceGrpc
                .newBlockingStub(managedChannel);
        AssignmentRequest request = AssignmentRequest.newBuilder().setRequest("Give me the Assignment plz").build();
        LOGGER.info("connected to server ");
        AssignmentResponse reply = assignmentServiceBlockingStub.getAssignment(request);
        LOGGER.info("We have the following consumers");
        for (Consumer c : reply.getConsumersList())
            LOGGER.info("consumer {}", c.getId());
        LOGGER.info("We have the following Assignment");
        for (Consumer c : reply.getConsumersList()) {
            LOGGER.info("Consumer {} has the following Assignment " , c.getId());
            for (Partition p : c.getAssignedPartitionsList()) {
                LOGGER.info("partition {}" ,  p.getId());
            }
        }
        managedChannel.shutdownNow();
        return reply.getConsumersList();
    }
}






