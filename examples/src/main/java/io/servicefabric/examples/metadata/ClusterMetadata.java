package io.servicefabric.examples.metadata;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ClusterConfiguration;
import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ClusterMessage;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.TransportMessage;
import io.servicefabric.transport.protocol.Message;

import rx.functions.Action1;
import rx.functions.Func1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Using Cluster metadata: metadata is set of custom paramters that may be used by application developers to attach
 * additional business information and identifications to cluster memebers.
 * 
 * <p>
 * in this example we see how to attach logical alias name to a cluster member we nick name Joe
 * 
 * @author ronen_h
 */
public class ClusterMetadata {

  private static final String MESSAGE_DATA = "hello/Joe";
  public static final Func1<ClusterMessage, Boolean> MESSAGE_PREDICATE = new Func1<ClusterMessage, Boolean>() {
    @Override
    public Boolean call(ClusterMessage t1) {
      return MESSAGE_DATA.equals(t1.message().data());
    }
  };

  public static void main(String[] args) {

    ICluster seedCluster = Cluster.newInstance(3000).join();

    // define the custom configuration meta data. and we add alias field.
    Map<String, String> metadata = new HashMap<>();
    metadata.put("alias", "Joe");
    ClusterConfiguration config =
        ClusterConfiguration.newInstance().port(4004).seedMembers("localhost" + ":" + "3000").memberId("my_member_id")
            .metadata(metadata);

    // configure cluster 2 with the metadata and attach cluster 2 as Joe and join seed
    ICluster joeCluster = Cluster.newInstance(config).join();

    // filter and subscribe on hello/joe and print the welcome message.
    joeCluster.listen().filter(MESSAGE_PREDICATE).subscribe(new Action1<ClusterMessage>() {
      @Override
      public void call(ClusterMessage t1) {
        System.out.println("Hello Joe");
      }
    });

    // get the list of members in the cluster and locate Joe tell Hello/Joe
    List<ClusterMember> members = seedCluster.membership().members();
    for (ClusterMember m : members) {
      if (m.metadata().containsKey("alias")) {
        if (m.metadata().get("alias").equals("Joe")) {
          seedCluster.send(m, new Message(MESSAGE_DATA));
        }
      }
    }
  }

}
