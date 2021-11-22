import java.util.HashMap;
import java.util.List;

import logist.plan.Plan;
import logist.task.Task;
import logist.simulation.Vehicle;
import logist.task.TaskSet;


/**********************************************************************************
 *
 *  Solution
 *
 **********************************************************************************/

public class Solution {


    // Solution's variables --------------------------------------------------------
    private HashMap<TypedTask,TypedTask>    nextTaskTask;
    private HashMap<Vehicle,TypedTask>      nextTaskVehicle;
    private HashMap<TypedTask, Integer>     timeMap;
    private HashMap<TypedTask, Vehicle>     vehicleMap;


    /******************************************************************************
     *
     *  Constructor
     *
     ******************************************************************************/

    public Solution() {
        nextTaskTask        = new HashMap<>();
        nextTaskVehicle     = new HashMap<>();
        timeMap             = new HashMap<>();
        vehicleMap          = new HashMap<>();
    }

    /******************************************************************************
     *
     *  SelectInitialCondition
     *
     ******************************************************************************/

    public static Solution SelectInitialCondition(List<Vehicle> vehicles, TaskSet tasks){
        Solution A = new Solution();

        // Select the biggest vehicle
        Vehicle biggest = vehicles.get(0);

        for(Vehicle v: vehicles){
            if (v.capacity() > biggest.capacity()){
                biggest = v;
            }
        }


        // Put all the task in it
        int count = 0;
        TypedTask t_previous = null;

        for(Task t : tasks){
            TypedTask pickup = new TypedTask(t, true);
            TypedTask delivery = new TypedTask(t, false);

            A.putNextTask(biggest, pickup);
            A.putNextTask(biggest, delivery);
            
            A.putNextTask(t_previous, pickup);
            A.putNextTask(pickup, delivery);

            A.setTime(pickup, count++);
            A.setTime(delivery, count++);

            A.setVehicle(pickup, biggest);
            A.setVehicle(delivery, biggest);

            t_previous = delivery;
        }

        return A;
    }



    /******************************************************************************
     *
     *  Planner
     *
     ******************************************************************************/

    public Plan toPlan(){
        return null;
    }

    /******************************************************************************
     *
     *  toString
     *
     ******************************************************************************/

    public String toString(){
        return " ";
    }

    /******************************************************************************
     *
     *  Getters and Setters
     *
     ******************************************************************************/

    public TypedTask nextTask(Vehicle v){   return nextTaskVehicle.get(v); }
    public TypedTask nextTask(TypedTask t){ return nextTaskTask.get(t); }
    public Integer time(TypedTask t){       return timeMap.get(t); }
    public Vehicle vehicle(TypedTask t) {   return vehicleMap.get(t); }




    private TypedTask putNextTask(Vehicle vKey, TypedTask tVal) {
        return nextTaskVehicle.put(vKey, tVal); }
    private TypedTask putNextTask(TypedTask tKey, TypedTask tVal) {
        return nextTaskTask.put(tKey, tVal); }
    private Integer setTime(TypedTask t, Integer i) {
        return timeMap.put(t, i); }
    private Vehicle setVehicle(TypedTask t, Vehicle v) {
        return vehicleMap.put(t, v);  }

}

