import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import logist.plan.Plan;
import logist.simulation.Vehicle;
import logist.task.Task;
import logist.task.TaskSet;
import logist.topology.Topology.City;

/**********************************************************************************
 *
 *  Solution
 *
 **********************************************************************************/

public class Solution {

	// we create a solution based on the available vehicles and tasks
	private List<Vehicle> vehicles; 
	private List<Task> tasks;
	
	//this is our encoded solution
	//solutions[i] -> the solution sequence for vehicles[i]
	//the sequence has the following rules:
	//Let N = size(tasks)
	//the numbers m in {0, 1, ..., N - 1} denote pickup of tasks[m]
	//the numbers n in {N, N + 1, ..., 2N - 1} denote delivery of tasks[n - N]
	// e.g 0, N   -> pickup, deliver task 0
	//     1, N+1 -> pickup, deliver task 1
	
	private ArrayList<ArrayList<Integer>> solutions;
	private ArrayList<Integer> costs;

    /******************************************************************************
     *
     *  Constructors
     *
     ******************************************************************************/
	
	public Solution(List<Vehicle> vehicles, TaskSet tasks) {
		this.vehicles = vehicles;

		// get all the tasks from Taskset to array
		this.tasks = new ArrayList<Task>();
		for(Task t: tasks) {
            this.tasks.add(t);
        }
		
		this.solutions = new ArrayList<ArrayList<Integer>>();
		//initalize empty sequences for each vehicle
		for(int i = 0; i < vehicles.size(); i++) {
			ArrayList<Integer> seq = new ArrayList<Integer>();
			solutions.add(seq);
		}
		
		costs = new ArrayList<Integer>();
		for(int i = 0; i < vehicles.size(); i++) {
			costs.add(Integer.MAX_VALUE);
		}
	}

	/***************************************************************************/

	public Solution(List<Vehicle> vehicles, List<Task> tasks) {
		this.vehicles = vehicles;

		this.tasks = tasks;


		this.solutions = new ArrayList<ArrayList<Integer>>();
		//initalize empty sequences for each vehicle
		for(int i = 0; i < vehicles.size(); i++) {
			ArrayList<Integer> seq = new ArrayList<Integer>();
			solutions.add(seq);
		}

		costs = new ArrayList<Integer>();
		for(int i = 0; i < vehicles.size(); i++) {
			costs.add(Integer.MAX_VALUE);
		}
	}

	/***************************************************************************/

	public Solution(Solution s) {
		this.vehicles = s.getVehicles();
		this.tasks = s.getTasks();
		
		this.costs = new ArrayList<Integer>(s.getCosts());
		this.solutions = new ArrayList<ArrayList<Integer>>();
		
		for(int i = 0; i < s.vehicles.size(); i++) {
			ArrayList<Integer> seq = new ArrayList<Integer>(s.getSolutions().get(i));
			this.solutions.add(seq);
		}
	}

    /******************************************************************************
     *
     *  generateInitialSolution
     *
     ******************************************************************************/

	public void generateInitialSolution() {
		
		//find the vehicle with the max capacity
		int vehicleMaxCapacityIndex = -1;
		for(int i = 0; i < vehicles.size(); i++) {
			if(vehicleMaxCapacityIndex == -1 || vehicles.get(i).capacity() > vehicles.get(vehicleMaxCapacityIndex).capacity())
				vehicleMaxCapacityIndex = i;
		}
		

		// generate the initial solution
		// the initial solution is to assign each task to the 
		// vehicle with the max capacity one by one, so the sequence becomes
		// for tasks [0,1,...N)
		// 0, N, 1, N + 1, 2, N + 2, ..., N-1, 2N-1
		// which means pickup task 0, deliver it, pick up task 1, deliver it, ...
		
		for(int i = 0; i < tasks.size(); i++) {
			ArrayList<Integer> vehMaxSeq = solutions.get(vehicleMaxCapacityIndex);
			vehMaxSeq.add(i);
			vehMaxSeq.add(i + tasks.size());
		}
		
		//first cost is just the cost of the vehicleMaxCapacity
		this.costs.set(vehicleMaxCapacityIndex, getVehicleCost(vehicles.get(vehicleMaxCapacityIndex), solutions.get(vehicleMaxCapacityIndex)));
	}

	/***************************************************************************/

	public int getVehicleCost(Vehicle vehicle, List<Integer> sequence) {
		// calculate the cost of the currently encoded solution of vehicle
		// e.g for vehicle's solution get current city,
		// from there follow the route encoded by integers.
		// return the total cost of the vehicle when the routes specified in solution[vehicleIndex] taken.
		
		double distance = 0;
		City currentCity = vehicle.getCurrentCity();
		for(int taskIndex: sequence) {
			if(taskIndex < tasks.size()) {
				distance += currentCity.distanceTo(tasks.get(taskIndex).pickupCity);
				currentCity = tasks.get(taskIndex).pickupCity;
			}else {
				distance += currentCity.distanceTo(tasks.get(taskIndex - tasks.size()).deliveryCity);
				currentCity = tasks.get(taskIndex - tasks.size()).deliveryCity;
			}
		}
		
		return (int) (vehicle.costPerKm()*distance);
	}

	/***************************************************************************/

	public int getCost() {
		// return the sum of the costs array
		int total = 0;
		for(int c: costs) {
			if(c != Integer.MAX_VALUE)
				total += c;
		}
		
		return total;
	}

	/***************************************************************************/

	public List<Plan> generatePlans(){
		// By looking at the current state of solutions so
		// the main code will use the return value to assign plans to each vehicle.
		
		List<Plan> plans = new ArrayList<Plan>();
		for(int i = 0; i < solutions.size(); i++) {
			City current = vehicles.get(i).getCurrentCity();
			Plan plan = new Plan(current);
			
			List<Integer> seq = solutions.get(i);
			for(int taskIndex: seq) {
				if(taskIndex < tasks.size()) {
					for(City c: current.pathTo(tasks.get(taskIndex).pickupCity)) {
						plan.appendMove(c);
					}
					plan.appendPickup(tasks.get(taskIndex));
					current = tasks.get(taskIndex).pickupCity;
				}else {
					for(City c: current.pathTo(tasks.get(taskIndex - tasks.size()).deliveryCity)) {
						plan.appendMove(c);
					}
					plan.appendDelivery(tasks.get(taskIndex - tasks.size()));
					current = tasks.get(taskIndex - tasks.size()).deliveryCity;
				}
			}
			//System.out.printf("Vehicle %d cost: %d %n", i, (int) (vehicles.get(i).costPerKm() * plan.totalDistance()));
			plans.add(plan);
		}
		

		
		return plans;
	}

	/***************************************************************************/

	public int getVehicleIndex(int taskIndex) {
		// Given a task, return its assigned vehicle by looking at each vehicle's solution
		// to see if it contains the task
		int v1Index = -1;
		for(int i = 0; i < solutions.size(); i++) {
			ArrayList<Integer> sol = solutions.get(i);
			if(sol.contains(taskIndex)) {
				return i;
			}
		}
		return -1;
	}

	/******************************************************************************
	 *
	 *  getRandomNeighbor
	 *
	 *  1. select a random task index t
	 *  2. identify the tasks's current assigned vehicle (v1) via solutions.get(i).contains
	 *  3. select a random vehicle index (v2)
	 *
	 *  remove the task (pickup, delivery) from v1's solution and try to add the task
	 *  to v2's solution in every possible way
	 *  this means delivery should happen after pickup in the sequence
	 *  and also we should check for every new solution if it is valid or not
	 *  with respect to weight constraints.
	 *  while creating the new solutions you should keep track of the one with the minimum cost
	 *  so we return that specific solution
	 *  important to work with copies, and not references.
	 *
	 ******************************************************************************/

	public Solution getRandomNeighbor(){
		
		Random rand = new Random();
		int taskIndex = rand.nextInt(tasks.size());
		int v1Index = getVehicleIndex(taskIndex);
		int v2Index = rand.nextInt(vehicles.size());
		
		
		
		int bestCost = Integer.MAX_VALUE;
		Solution bestSolution = this;
		

		Solution solutionMinusTask = new Solution(this);
		
		
		solutionMinusTask.getSolutions().get(v1Index).remove(Integer.valueOf(taskIndex));
		solutionMinusTask.getSolutions().get(v1Index).remove(Integer.valueOf(taskIndex + tasks.size()));
		// Calculate the cost without this task for v1
		int withoutV1Cost = getVehicleCost(solutionMinusTask.getVehicles().get(v1Index),
											solutionMinusTask.getSolutions().get(v1Index));
		solutionMinusTask.getCosts().set(v1Index, withoutV1Cost);

		
		List<Integer> v2Seq = solutionMinusTask.getSolutions().get(v2Index);

		
		for(int p = 0; p < v2Seq.size() + 1; p++) {
			for(int d = p + 1; d < v2Seq.size() + 2; d++) {
				Solution solutionPlusTaskAgain = new Solution(solutionMinusTask);
				solutionPlusTaskAgain.getSolutions().get(v2Index).add(p, taskIndex);
				solutionPlusTaskAgain.getSolutions().get(v2Index).add(d, taskIndex + tasks.size());
				
				//check if valid
				if(canCarry(solutionPlusTaskAgain.getVehicles().get(v2Index),
							solutionPlusTaskAgain.getSolutions().get(v2Index))) {

					int withV2Cost = getVehicleCost(solutionPlusTaskAgain.getVehicles().get(v2Index),
													solutionPlusTaskAgain.getSolutions().get(v2Index));

					solutionPlusTaskAgain.getCosts().set(v2Index, withV2Cost);
					
					if(withV2Cost < bestCost) {
						bestCost = withV2Cost;
						bestSolution = solutionPlusTaskAgain;
					}
				}
			}
		}
		
		return bestSolution;
	}

	/***************************************************************************/

	public boolean canCarry(Vehicle vehicle, List<Integer> sequence) {
		// Compute whether the plan is valid
		// with respect to the capacity of the vehicle
		int capacity = vehicle.capacity();
		int currentWeight = 0;
		for(int taskId: sequence){
			if ( taskId >= tasks.size()){
				currentWeight -= tasks.get(taskId - tasks.size()).weight;
			}else{
				currentWeight += tasks.get(taskId).weight;
				if (currentWeight > capacity){
					return false;
				}
			}
		}
		return true;
	}

    /******************************************************************************
     *
     *  Getters And Setters
     *
     ******************************************************************************/

	public List<Vehicle> getVehicles(){
		return vehicles;
	}
	public void setVehicles(List<Vehicle> vehicles){
		this.vehicles = vehicles;
	}
	public List<Task> getTasks(){
		return tasks;
	}
	public void setTasks(List<Task> tasks){
		this.tasks = tasks;
	}
	public ArrayList<ArrayList<Integer>> getSolutions(){
		return solutions;
	}
	public void setSolutions(ArrayList<ArrayList<Integer>> solutions){
		this.solutions = solutions;
	}
	public ArrayList<Integer> getCosts(){
		return costs;
	}
	public void setCosts(ArrayList<Integer> cost){
		this.costs = cost;
	}
	
}
