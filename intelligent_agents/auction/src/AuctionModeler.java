//the list of imports

import logist.agent.Agent;
import logist.behavior.AuctionBehavior;
import logist.plan.Plan;
import logist.simulation.Vehicle;
import logist.task.Task;
import logist.task.TaskDistribution;
import logist.task.TaskSet;
import logist.topology.Topology;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * A auction agent that model the opponent
 *
 */
@SuppressWarnings("unused")
// java -jar ../logist/logist.jar config/auction.xml auction-modeler
public class AuctionModeler implements AuctionBehavior {

    private Topology topology;
    private TaskDistribution distribution;
    private Agent agent;
    private Random random;
    private List<Vehicle> vehicles;
    private List<Task> winnedTasks;
    private int currentCost;
    private int newCost;
    private int[] opponentCost;
    private int[] newOpponentCost;
    private int[] marginalOpponentCost;
    private Long[] opponentReward;



    private List<List<Task>> adversaryTask;


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

        this.turn = 0;
        this.countFail = 0;

    }

    @Override
    public Long askPrice(Task task) {

        double marginalCost;
        int miniMarginal = 0;

        double miniCost = Double.MAX_VALUE;
        if (turn == 0){
            /*** Tries to win the first task ******************************************************/
            for (Vehicle v: this.vehicles){
                marginalCost = (v.getCurrentCity().distanceUnitsTo(task.pickupCity) + task.pickupCity.distanceTo(task.deliveryCity))*v.costPerKm();
                if (miniCost > marginalCost){
                    miniCost = marginalCost;
                }
            }
            marginalCost = miniCost;

        }
        else{
            /*** Marginal Cost ********************************************************************/
            // Calculate the marginal cost for all the agent in the map from previously recorded task

            miniMarginal = Integer.MAX_VALUE;

            for (int i = 0 ; i < nb_player ; i++){
                List<Task> futureTasks = new LinkedList<Task>(adversaryTask.get(i));
                futureTasks.add(task);
                Planner planner = new Planner(this.vehicles, futureTasks);
                List<Plan> _ = planner.simulatedAnnealingSearch();

                newOpponentCost[i] = planner.getCost();
                marginalOpponentCost[i] = Math.abs(newOpponentCost[i] - opponentCost[i]);
                System.out.println("marginalOpponentCost["+i+"] = " + marginalOpponentCost[i]);
                if (miniMarginal > marginalOpponentCost[i]){
                    miniMarginal = marginalOpponentCost[i];
                }
            }

            marginalCost = marginalOpponentCost[this.agent.id()];
            if(marginalCost == 0) marginalCost = 200;
        }
        /*** Bid **********************************************************************************/
        // Still need to find a strategy that use all this data

        long finalBid = 0;

        Random r = new Random();


        if (turn <= 5){
            finalBid = (long) (marginalCost);
        } else {
            double ratio = 1 + r.nextDouble();
            long bid = (long) (marginalCost * ratio);
            finalBid = bid;
        }


        System.out.println("Agent Modeler will bid :" + finalBid);
        return finalBid;
    }

    @Override
    public void auctionResult(Task previous, int winner, Long[] bids) {


        /*** Initialisation  **********************************************************************/
        // Initialisation of the arrays to record the most of data

        if (turn == 0){
            nb_player = bids.length;
            opponentCost = new int[nb_player];
            newOpponentCost = new int[nb_player];
            marginalOpponentCost = new int[nb_player];
            opponentReward = new Long[nb_player];
            adversaryTask = new ArrayList<>();
            for (int i = 0 ; i < nb_player ; i++){
                adversaryTask.add(i, new LinkedList<>());
                opponentReward[i] = Long.valueOf(0);
                opponentCost[i] = 0;
                newOpponentCost[i] = 0;
                marginalOpponentCost[i] = 0;
            }
        }


        /*** Update Model for the winner **********************************************************/
        // The winner get a new task and it cost is now the cost calculated for the bid

        adversaryTask.get(winner).add(previous);
        opponentCost[winner] = newOpponentCost[winner];
        opponentReward[winner] += bids[winner];

        if (winner == this.agent.id()) {
            System.out.printf("Agent %d won task %d with reward %d %n", winner, previous.id, previous.reward);
            countFail = 0;
        }else{
            countFail += 1;
        }
        turn += 1;

    }


    @Override
    public List<Plan> plan(List<Vehicle> vehicles, TaskSet tasks) {
        Planner planner = new Planner(vehicles, tasks);
        List<Plan> plans = planner.simulatedAnnealingSearch();
        System.out.printf("This was the plan for agent %d with reward %d and cost %d %n %n", agent.id(), tasks.rewardSum(), planner.getCost());
        return plans;
    }

}
