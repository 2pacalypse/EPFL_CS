//the list of imports

import logist.agent.Agent;
import logist.behavior.AuctionBehavior;
import logist.plan.Plan;
import logist.simulation.Vehicle;
import logist.task.Task;
import logist.task.TaskDistribution;
import logist.task.TaskSet;
import logist.topology.Topology;
import logist.topology.Topology.City;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A very simple auction agent that assigns the tasks to the closest vehicle to the pickup city
 *
 */
@SuppressWarnings("unused")
// java -jar ../logist/logist.jar config/auction.xml auction-state
public class AuctionState implements AuctionBehavior {

    private Topology topology;
    private TaskDistribution distribution;
    private Agent agent;
    private Random random;
    private List<Vehicle> vehicles;
    private List<Task> winnedTasks;
    private int currentCost;
    private int newCost;
    private Long[] Ebids;
    private Long[] minBids;
    private int countFail;
    private int turn;
    private int nb_player;



    @Override
    public void setup(Topology topology, TaskDistribution distribution,
                      Agent agent) {

        this.topology = topology;
        this.distribution = distribution;
        this.agent = agent;
        this.vehicles = agent.vehicles();
        this.winnedTasks = new ArrayList<>();
        this.currentCost = 0;
        this.Ebids = new Long[10];
        this.minBids = new Long[10];
        this.turn = 0;
        this.countFail = 0;
        for (int i = 0 ; i< 10; i++){
            minBids[i] = Long.MAX_VALUE;
            Ebids[i] = Long.valueOf(0);
        }
    }

    @Override
    public Long askPrice(Task task) {

        /*** Marginal Cost ************************************************************************/
        List<Task> futureTasks = new ArrayList<Task>(winnedTasks);
        futureTasks.add(task);
        Planner planner = new Planner(this.vehicles, futureTasks);
        List<Plan> _ = planner.simulatedAnnealingSearch();

        this.newCost = planner.getCost();

        double marginalCost = Math.abs(this.newCost - this.currentCost);
        if(marginalCost == 0) marginalCost = 100;



        /*** Bid **********************************************************************************/

        long finalBid = 0;
        long possibleWorkingBid = Long.MAX_VALUE;
        if (countFail > 2){
            long miniBin = Long.MAX_VALUE;
            for (int i = 0; i < nb_player; i++){
                if (Ebids[i] < miniBin){
                    miniBin = Ebids[i];
                }
            }
            possibleWorkingBid = miniBin-1;
        }
        Random r = new Random();


        if (turn <= 5){
            finalBid = (long) (marginalCost);
        }else{
            double ratio = 1 + r.nextDouble()*0.5;
            long bid = (long) Math.ceil(ratio * marginalCost);
            finalBid = bid;
        }

        if (finalBid  < possibleWorkingBid){
            return finalBid;
        }
        return possibleWorkingBid;
    }

    @Override
    public void auctionResult(Task previous, int winner, Long[] bids) {
        turn += 1;
        nb_player = bids.length;
        for (int i = 0; i < bids.length; i++){
            Ebids[i] = (Ebids[i]+bids[i])/2;
            System.out.print(Ebids[i] + ", ");
        }

        if (winner == this.agent.id()) {
            System.out.printf("Agent %d won task %d with reward %d %n", winner, previous.id, previous.reward);
            this.winnedTasks.add(previous);
            this.currentCost = this.newCost;
            countFail = 0;
        }else{
            countFail += 1;
        }

    }


    @Override
    public List<Plan> plan(List<Vehicle> vehicles, TaskSet tasks) {
        Planner planner = new Planner(vehicles, tasks);
        List<Plan> plans = planner.simulatedAnnealingSearch();
        System.out.printf("This was the plan for agent %d with reward %d and cost %d %n %n", agent.id(), tasks.rewardSum(), planner.getCost());

        return plans;
    }

}
