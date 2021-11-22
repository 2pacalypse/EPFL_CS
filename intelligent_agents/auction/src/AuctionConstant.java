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
public class AuctionConstant implements AuctionBehavior {

	private Topology topology;
	private TaskDistribution distribution;
	private Agent agent;
	private List<Vehicle> vehicles;
	private HashMap<Vehicle,List<Task>> tasksPlan;

	private HashMap<Vehicle,City> currentCities;

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
		for (Vehicle vehicle : vehicles){
			currentCities.put(vehicle, vehicle.homeCity());
			tasksPlan.put(vehicle, new ArrayList<>());
		}
	}

	@Override
	public void auctionResult(Task previous, int winner, Long[] bids) {

		if (winner == agent.id()) {
			currentCities.put(bestVehicleForTask, previous.deliveryCity);
			tasksPlan.get(bestVehicleForTask).add(previous);
		}
	}
	
	@Override
	public Long askPrice(Task task) {

		long minDist = Long.MAX_VALUE;
		for (Vehicle vehicle : vehicles ){
			if (vehicle.capacity() > task.weight){
				long distanceToTask = currentCities.get(vehicle).distanceUnitsTo(task.pickupCity);

				if (distanceToTask < minDist){

					minDist = distanceToTask;
					bestVehicleForTask = vehicle;
				}
			}
		}

		long distanceTask = task.pickupCity.distanceUnitsTo(task.deliveryCity);

		long distanceSum = distanceTask + minDist;
		double marginalCost = Measures.unitsToKM(distanceSum * bestVehicleForTask.costPerKm());

		//System.out.println("Constant :" +marginalCost);
		double ratio = 1.5;
		double bid = ratio * marginalCost;

		return (long) Math.round(bid);
	}

	@Override
	public List<Plan> plan(List<Vehicle> vehicles, TaskSet tasks) {

		List<Plan> plans = new ArrayList<>();

		for (Vehicle vehicle: vehicles){
			Plan planVehicle = naivePlan(vehicle, tasksPlan.get(vehicle));
			plans.add(planVehicle);
		}



		return plans;
	}

	private Plan naivePlan(Vehicle vehicle, List<Task> tasksPlan) {

		City current = vehicle.getCurrentCity();
		Plan plan = new Plan(current);

		for (Task task : tasksPlan) {
			// move: current city => pickup location
			for (City city : current.pathTo(task.pickupCity))
				plan.appendMove(city);

			plan.appendPickup(task);

			// move: pickup location => delivery location
			for (City city : task.path())
				plan.appendMove(city);

			plan.appendDelivery(task);

			// set current city
			current = task.deliveryCity;
		}
		return plan;
	}
}
