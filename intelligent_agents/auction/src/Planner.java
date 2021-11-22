import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import logist.plan.Plan;
import logist.simulation.Vehicle;
import logist.task.Task;
import logist.task.TaskSet;

public class Planner {

	private List<Vehicle> vehicles;
	private  List<Task> tasks;
	private int cost;
	
	public Planner(List<Vehicle> vehicles, TaskSet tasks) {
		this.vehicles = vehicles;
		this.tasks = new ArrayList<Task>();
		for(Task t: tasks) {
			this.tasks.add(t);
		}
	}

	public Planner(List<Vehicle> vehicles,  List<Task> tasks) {
		this.vehicles = vehicles;
		this.tasks = tasks;
	}
	
	 private double changeTemperature(double before) {
	    	return before - 1; // linear  decrease
	    	//return before / 2; // expo decrease
	    	
	    }
	    
	    
	public List<Plan> simulatedAnnealingSearch() {
		if(tasks.isEmpty()) {
			List<Plan> plans = new ArrayList<Plan>();
			while (plans.size() < vehicles.size())
				plans.add(Plan.EMPTY);
			return plans;
		}
		
		Solution current = new Solution(vehicles, tasks);
		current.generateInitialSolution();
		//double temperature = Math.pow(2, 100);
		double temperature = 1000 + 1;
		
		
		int i = 0;
		while(true) {
			if(temperature <= 1) {
				//System.out.printf("Total cost is: %d with %d iterations%n", current.getCost(), i);
				cost = current.getCost();
				return current.generatePlans();
			}
			
			Solution randomSolution = current.getRandomNeighbor();
			int delta = randomSolution.getCost() - current.getCost();
			//System.out.println(delta);
			//System.out.printf("rand: %d current: %d %n",randomSolution.getCost(), current.getCost());
			if(delta < 0) {
				current = randomSolution;
			}else {	
				Random rand = new Random();
				double coin = rand.nextDouble();
				double p = Math.exp(-delta/ (double)temperature);
				if(coin < p) {	
					current = randomSolution;
				}
			}
		
			temperature  = changeTemperature(temperature);
			i++;
		}
		
	}

	public List<Vehicle> getVehicles() {
		return vehicles;
	}

	public void setVehicles(List<Vehicle> vehicles) {
		this.vehicles = vehicles;
	}

	public List<Task> getTasks() {
		return tasks;
	}

	public void setTasks(List<Task> tasks) {
		this.tasks = tasks;
	}

	public int getCost() {
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}


}
