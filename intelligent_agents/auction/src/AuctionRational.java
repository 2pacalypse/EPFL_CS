//the list of imports
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import logist.Measures;
import logist.behavior.AuctionBehavior;
import logist.agent.Agent;
import logist.simulation.Vehicle;
import logist.plan.Plan;
import logist.task.Task;
import logist.task.TaskDistribution;
import logist.task.TaskSet;
import logist.topology.Topology;
import logist.topology.Topology.City;

/**
 * A very simple auction agent that assigns all tasks to its first vehicle and
 * handles them sequentially.
 * 
 */
@SuppressWarnings("unused")
public class AuctionRational implements AuctionBehavior {

	private Topology topology;
	private TaskDistribution distribution;
	private Agent agent;
	private Random random;
	
	private long opponentBid;
	private long ourBid;
	private int rounds = 0;
	
	private int NROUNDS = 10;

	@Override
	public void setup(Topology topology, TaskDistribution distribution,
			Agent agent) {

		this.topology = topology;
		this.distribution = distribution;
		this.agent = agent;
		this.random = new Random();

	}

	@Override
	public void auctionResult(Task previous, int winner, Long[] bids) {
		long min = Long.MAX_VALUE;
		
		if (winner == agent.id()) {
			System.out.printf("Agent %d won task %d with reward %d %n", winner, previous.id, previous.reward);
		}else {
			
			for(int i = 0; i < bids.length; i++) {
				if(i != agent.id()) {
					if(bids[i] != null &&  bids[i] < min) {	
						min = bids[i];
					}
				}
			}
		}
		
		opponentBid = min;
	}
	
	@Override
	public Long askPrice(Task task) {
		
		
		
		for(Vehicle v: agent.vehicles()) 
			if(v.capacity() >= task.weight) {
					
				if(rounds < NROUNDS && rounds != 0) {
					rounds++;
					if(opponentBid < ourBid) {
						ourBid /= 2;
					}else {
						ourBid++;
					}
						
					return ourBid;
				}
				rounds++;
				
				//System.out.println(rounds);
				
				TaskSet t = agent.getTasks().clone();
				Planner withoutTask = new Planner(agent.vehicles(), t);

				List<Plan> _ = withoutTask.simulatedAnnealingSearch();	
				
				t.add(task);
				
				
				Planner withTask = new Planner(agent.vehicles(), t);
				
				List<Plan> __ = withTask.simulatedAnnealingSearch();
				int costDiff = withTask.getCost() - withoutTask.getCost();
				
				
				//System.out.println("Rewards : " + agent.getTasks().rewardSum());
				//System.out.println("Costs " + withTask.getCost());
				//System.out.println("CostDiff " + costDiff);
				
				
				
				
				
				
				ourBid = costDiff > 0 ? costDiff + 1 : 0 ;
				return (long) (ourBid);
			}
		
		
		return null;
	}

	@Override
	public List<Plan> plan(List<Vehicle> vehicles, TaskSet tasks) {
		
//		System.out.println("Agent " + agent.id() + " has tasks " + tasks);
		
		long time_start = System.currentTimeMillis();
		 
		Planner planner = new Planner(vehicles, tasks);
       
        List<Plan> plans = planner.simulatedAnnealingSearch(); 
        
        long time_end = System.currentTimeMillis();
        long duration = time_end - time_start;
        
        System.out.println("The plan was generated in "+duration+" milliseconds.");
        System.out.printf("This was the plan for agent %d with reward %d and cost %d %n %n", agent.id(), tasks.rewardSum(), planner.getCost());
         
        
		return plans;
		

	}

	private Plan naivePlan(Vehicle vehicle, TaskSet tasks) {
		City current = vehicle.getCurrentCity();
		Plan plan = new Plan(current);

		for (Task task : tasks) {
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
