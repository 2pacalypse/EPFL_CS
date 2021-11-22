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
public class AuctionRisky implements AuctionBehavior {

    private Topology topology;
    private TaskDistribution distribution;
    private Agent agent;
    private List<Vehicle> vehicles;
    private HashMap<Vehicle,List<Task>> tasksPlan;

    private HashMap<Vehicle,City> currentCities;
    private long costPerKmMean;


    private Vehicle bestVehicleForTask;

    @Override
    public void setup(Topology topology, TaskDistribution distribution,
                      Agent agent) {
        this.topology = topology;
        this.distribution = distribution;
        this.agent = agent;
        this.vehicles = agent.vehicles();
        this.currentCities = new HashMap<>();
        this.tasksPlan = new HashMap<>();
        int count = 0;
        this.costPerKmMean = 0;
        for (Vehicle vehicle : vehicles){
            currentCities.put(vehicle, vehicle.homeCity());
            tasksPlan.put(vehicle, new ArrayList<>());
            count+=1;
            costPerKmMean+=vehicle.costPerKm();
        }

        costPerKmMean /= count;
    }

    @Override
    public void auctionResult(Task previous, int winner, Long[] bids) {

        if (winner == agent.id()) {
            System.out.println("AuctionRisky is winner");
        }

    }

    @Override
    public Long askPrice(Task task) {

        long distanceTask = task.pickupCity.distanceUnitsTo(task.deliveryCity);

        double marginalCost = Measures.unitsToKM(distanceTask * costPerKmMean);

        double ratio = 0.89;
        double bid = ratio * marginalCost;

        return (long) Math.round(bid);
    }

    @Override
    public List<Plan> plan(List<Vehicle> vehicles, TaskSet tasks) {
        Planner planner = new Planner(vehicles, tasks);
        List<Plan> plans = planner.simulatedAnnealingSearch();
        System.out.printf("This was the plan for agent %d with reward %d and cost %d %n %n", agent.id(), tasks.rewardSum(), planner.getCost());

        return plans;


    }

}
