//the list of imports

import logist.Measures;
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
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * A very simple auction agent that assigns the tasks to the closest vehicle to the pickup city
 *
 */
@SuppressWarnings("unused")
// java -jar ../logist/logist.jar config/auction.xml auction-constant
public class AuctionEspecter implements AuctionBehavior {

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
    private Double[] ratios;



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
        this.ratios = new Double[10];
        this.minBids = new Long[10];

        for (int i = 0 ; i< 10; i++){
            minBids[i] = Long.MAX_VALUE;
            Ebids[i] = Long.valueOf(0);
            ratios[i] = Double.valueOf(0);
        }
        ratios[this.agent.id()] = Double.valueOf(1);
    }

    @Override
    public Long askPrice(Task task) {
        List<Task> futureTasks = new ArrayList<Task>(winnedTasks);
        futureTasks.add(task);
        Planner planner = new Planner(this.vehicles, futureTasks);
        List<Plan> _ = planner.simulatedAnnealingSearch();

        this.newCost = planner.getCost();

        double marginalCost = Math.abs(this.newCost - this.currentCost);
        if(marginalCost == 0) marginalCost = 100;
        //System.out.println("Test : " +marginalCost);

        Random r = new Random();

        double ratio = 1 + r.nextDouble();
        //System.out.println("randomRatio : " +ratio);


        double expectation = getExpectation(task.pickupCity, task.deliveryCity);

        long bid = (long) Math.ceil((ratio - expectation) * marginalCost);
        long finalBid = 0;
        for (int i = 0; i < 10; i++){
            if (i == this.agent.id()) {
                finalBid += ratios[i] * bid;
            }
            else{
                finalBid += ratios[i] * minBids[i];

            }
        }
        return finalBid;
    }

    @Override
    public void auctionResult(Task previous, int winner, Long[] bids) {

        for (int i = 0; i < bids.length; i++){
            Ebids[i] = (Ebids[i]+bids[i])/2;
            if (minBids[i] > bids[i]) {
                minBids[i] = bids[i];
            }
            System.out.print(bids[i] + ", ");
        }
        System.out.println("");

        System.out.println(winner);
        if (winner == this.agent.id()) {
            System.out.printf("Agent %d won task %d with reward %d %n", winner, previous.id, previous.reward);
            this.winnedTasks.add(previous);
            this.currentCost = this.newCost;
        }else{
            ratios[winner] *= 2;
        }

        // Renormalization of the ratio
        double sum = 0;
        for (int i = 0; i < bids.length; i++){
            sum += ratios[i];
        }
        for (int i = 0; i < bids.length; i++){
            ratios[i] /= sum;
        }
    }

    /*
     * Return the probability that a new task arrive on the path to the destination
     *
     */
    public double getExpectation(City from, City to) {

        int nbCities = from.pathTo(to).size() - 1;
        double p = 1.0;

        for(City c : from.pathTo(to)) {
            if(c == from) continue;

            p *= distribution.probability(c, to);
        }

        return p;
    }

    @Override
    public List<Plan> plan(List<Vehicle> vehicles, TaskSet tasks) {
        Planner planner = new Planner(vehicles, tasks);
        List<Plan> plans = planner.simulatedAnnealingSearch();
        System.out.printf("This was the plan for agent %d with reward %d and cost %d %n %n", agent.id(), tasks.rewardSum(), planner.getCost());

        return plans;
    }

}
