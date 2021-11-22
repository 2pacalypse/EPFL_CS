import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import logist.plan.Action;
import logist.plan.Action.Delivery;
import logist.plan.Action.Move;
import logist.plan.Action.Pickup;

import logist.plan.Plan;
import logist.plan.PlanVerifier;
import logist.simulation.Vehicle;
import logist.task.Task;
import logist.task.TaskSet;
import logist.topology.Topology.City;

/**********************************************************************************
 *
 *  Comparator to sort State by f value
 *
 **********************************************************************************/

class fComparator implements Comparator<State> {
    @Override
    public int compare(State a, State b) {
        return a.compareF(b);
    }
}



/**********************************************************************************
 *
 *  State
 *
 **********************************************************************************/

public class State {

    // State variables ------------------------------------------------------------
    private City        position;           // The position of the Agent on the map (ie the city)
    private TaskSet     carried;            // The set of task currently carried by the agent
    private TaskSet     tasks;              // The set of tasks currently in the environment

    // Utilities ------------------------------------------------------------------
    private Vehicle     vehicle;            // The vehicle that is on this state
    private int         carriedWeight;      // The carried weight
    // It's still needed to take into account the weigth of the packages

    // Graph related variables ----------------------------------------------------
    private State       parent;             // The parent of the state;
    private Action      action;             // The action that led to this state
    private Double      gCost;              // The g cost calculated from the start


    /******************************************************************************
     *
     *  Constructors
     *
     ******************************************************************************/

    // Constructor for the initial node
    public State(Vehicle vehicle,
                 City city,
                 TaskSet carriedTasks,
                 TaskSet availableTasks){

        
    	
    	
    	this.position   = city;
        this.carried    = carriedTasks;
        this.tasks      = availableTasks;

        this.vehicle    = vehicle;
        carriedWeight   = 0;

        this.gCost      = 0.;
        this.parent     = null;
        this.action     = null;

        


    }

    // Constructor for the other nodes
    public State(State parent,
                 Action action,
                 City city,
                 TaskSet carriedTasks,
                 TaskSet availableTasks){

        this.position   = city;
        this.carried    = carriedTasks;
        this.tasks      = availableTasks;

        this.vehicle    = parent.getVehicle();
        carriedWeight   = carried.weightSum();

        this.parent     = parent;
        this.action     = action;
        this.gCost      = parent.getgCost() + position.distanceTo(parent.getPosition())*vehicle.costPerKm();
    }

    /******************************************************************************
     *
     *  Test if the state is a final state
     *
     ******************************************************************************/

    public boolean isFinal(){
        return tasks.isEmpty() && carried.isEmpty();
    }

    /******************************************************************************
     *
     *  This function defines the heuristic
     *
     ******************************************************************************/

    public double getH(){
        double H = 0;
        double tmpCost;
        for (Task availableTask : tasks) {
            tmpCost = (position.distanceTo(availableTask.pickupCity)+availableTask.pickupCity.distanceTo(availableTask.deliveryCity)) *vehicle.costPerKm();
            if(H < tmpCost) H = tmpCost;
        }
        for(Task carriedTask: carried){
            tmpCost = position.distanceTo(carriedTask.deliveryCity)*vehicle.costPerKm();
            if(H < tmpCost) H = tmpCost;
        }
        return H;
    	
 
    }

    public static double f(State s){
        return s.getgCost()+s.getH();
    }

    public int compareF(State b){
        return Double.compare(f(this), f(b));
    }

    /******************************************************************************
     *
     *  Return the list of child states of the current state, the list is non-sorted
     *
     ******************************************************************************/

    public List<State> getChildStates(){
        List<State> states = new LinkedList<State>();
        
        // Deliver a package
        // The vehicle can deliver a package if it is on the right city
        for(Task carriedTask : carried){
            if (position == carriedTask.deliveryCity){
                TaskSet newCarried = carried.clone();
                Delivery deliveryAction = new Delivery(carriedTask);
                newCarried.remove(carriedTask);
                states.add(new State(this, deliveryAction, position, newCarried, tasks));
                break;
            }
        }

        
        for(Task availableTask : tasks){
            if ((position == availableTask.pickupCity) && (availableTask.weight <= vehicle.capacity()-carriedWeight)){
                TaskSet newTasks = tasks.clone();
                TaskSet newCarried = carried.clone();
                Pickup pickupAction = new Pickup(availableTask);
                newTasks.remove(availableTask);
                newCarried.add(availableTask);
                states.add(new State(this, pickupAction, position, newCarried, newTasks));
            }
        }
        

        // Move from a city to another
        // The vehicle can move to all the neighborhood cities
        for (City city : position.neighbors()){
            Move moveAction = new Move(city);
            states.add(new State(this, moveAction, city, carried, tasks));
        }

       

        // Pickup a package
        // The vehicle can pickup a package if it is on the right city and the weight doesn't exced th capacity

        return states;
    }

    /******************************************************************************
     *
     *  Generete a plan from the initial state to this state by backpropagation
     *
     ******************************************************************************/

    public Plan generatePlanToThis(){
        
    	Stack<Action> actions = new Stack<Action>();
        State n = this;
        // Backtracking from final state to the start to get all the actions
        do{
            actions.push(n.getAction());
            n = n.getParent();
        }while(n.getParent() != null);
        

        Plan plan = new Plan(n.getPosition());
        // Put all these action in order in the plan
        
        
        
        while(!actions.empty()){
            plan.append(actions.pop());
        }
        

        System.out.println("\n\n");
        
        System.out.println(plan);
        return plan;
    }

    /******************************************************************************
     *
     *  equals : two states are equals if they represent the same position, carried
     *      package and available packages
     *
     ******************************************************************************/

    @Override
    public boolean equals(Object other) {
        if (other instanceof State) {
            State otherState = (State) other;
            
            return (otherState.getPosition().equals(position) &&
                    (otherState.getCarried().equals(carried)) &&
                    (otherState.getTasks()).equals(tasks));
        }
        return false;
    }
    
    
    @Override 
    public int hashCode() {
		return position.hashCode() + carried.hashCode()*37 + tasks.hashCode()*37*37;
    	
    }

    /******************************************************************************
     *
     *  Getters and Setters
     *
     ******************************************************************************/

    public City getPosition() {
        return position;
    }

    public void setPosition(City position) {
        this.position = position;
    }

    public TaskSet getCarried() {
        return carried;
    }

    public void setCarried(TaskSet carried) {
        this.carried = carried;
    }

    public TaskSet getTasks() {
        return tasks;
    }

    public void setTasks(TaskSet tasks) {
        this.tasks = tasks;
    }

    public Vehicle getVehicle() {
        return vehicle;
    }

    public void setVehicle(Vehicle vehicle) {
        this.vehicle = vehicle;
    }

    public Double getgCost() {
        return gCost;
    }

    public void setgCost(Double gCost) {
        this.gCost = gCost;
    }

    public State getParent() {
        return parent;
    }

    public void setParent(State parent) {
        this.parent = parent;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    /******************************************************************************
     *
     *  toString
     *
     ******************************************************************************/

    @Override
    public String toString() {
        String str = "Action(" + action + ")\n";
        str += "{ pos(" + position + "),\n";
        str += "carried(";
        for(Task carriedTask : carried){
            str += carriedTask + ", ";
        }
        str += "),\n ";
        str += "tasks(";
        for(Task availableTask : tasks){
            str += availableTask + ", ";
        }
        str += ") }\n";
        return str;
    }
}
