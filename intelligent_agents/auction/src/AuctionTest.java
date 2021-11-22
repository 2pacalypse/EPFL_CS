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

import java.lang.reflect.Array;
import java.util.*;


@SuppressWarnings("unused")
// java -jar ../logist/logist.jar config/auction.xml auction-test

/**************************************************************************************************
 *
 *  taskComparator
 *
 *  Comparator to sort all the task by probability in the PriorityQueue<Task> taskSorted of the
 *  Agent
 *
 **************************************************************************************************/

class taskComparator implements Comparator<Task> {
    private TaskDistribution distribution;
    public taskComparator(TaskDistribution distribution){
        this.distribution = distribution;
    }
    @Override
    public int compare(Task a, Task b) {
        Double pa = distribution.probability(a.pickupCity, a.deliveryCity);
        Double pb = distribution.probability(b.pickupCity, b.deliveryCity);
        return Double.compare(pb, pa); // permutation of pb and pa on purpose to have a decreasing order
    }
}

/**************************************************************************************************
 *
 *  Auction Agent
 *
 *  Look in the future one step ahead, and mesure it chance of wining against the opponent.
 *
 **************************************************************************************************/

public class AuctionTest implements AuctionBehavior {

    private Agent agent;
    private List<Vehicle> vehicles;
    private List<Task> winnedTasks;
    private int currentCost;
    private int newCost;
    private List<List<Long>> bibsHistory;

    private int nbPossibleTask;
    private int bestTaskSize;
    private ArrayList<Task> bestTask;

    private int turn;
    private int nb_player;

    private Long minimumBid;

    /**********************************************************************************************
     *
     *  setup
     *
     **********************************************************************************************/

    @Override
    public void setup(Topology topology, TaskDistribution distribution, Agent agent) {

        this.agent = agent;
        this.vehicles = agent.vehicles();

        this.winnedTasks = new ArrayList<>();
        this.currentCost = 0;

        // Array of the all the tasks ranked by probability
        PriorityQueue<Task> taskSorted = new PriorityQueue<>(topology.size()*topology.size(),
                                                                new taskComparator(distribution));
        int id = -1; // decreasing id for no id overlap with the real task
        for (City from : topology.cities()) {
            for (City to : topology.cities()) {
                taskSorted.add(new Task(id--, from, to, 0, vehicles.get(0).capacity() / 3));
            }
        }

        // Array of the 'bestTaskSize' probable task ranked by probability
        this.bestTaskSize = 20;
        this.bestTask = new ArrayList<>(bestTaskSize);
        for (int i = 0 ; i < bestTaskSize ; i++){
            bestTask.add(taskSorted.poll());
        }

        // number of future task used to compute the expected future marginal cost.
        this.nbPossibleTask = 5;

        // Minimum possible Bid
        this.minimumBid = 750L;
    }

    /**********************************************************************************************
     *
     *  askPrice
     *
     **********************************************************************************************/

    @Override
    public Long askPrice(Task task) {

        Long bid = 0L;

        /*** Constant bid for the first tasks *****************************************************/
        // The number of turn before switching strategy (here 7) is a hyperparameter
        // The constant bid is a hyperparameter too

        if (turn < 7){
            return minimumBid;
        }

        /*** Marginal Cost ************************************************************************/
        // Calculate the marginal cost of the task :
        // Cost of the plan with the task - Cost of the plan without the task

        ArrayList<Task> ifTaskWinned = new ArrayList<Task>(winnedTasks);
        ifTaskWinned.add(task);
        Planner planner = new Planner(this.vehicles, ifTaskWinned);
        List<Plan> _ = planner.simulatedAnnealingSearch();
        newCost = planner.getCost();
        Long marginalCost = (long) newCost - currentCost;

        /*** One-step-look-ahead ******************************************************************/
        // one-step-look-ahead (1SLA) method
        // We look at the effect of wining the task on the futur marginal cost
        // Some Task could be not interesting, so we want to avoid them by biding more than it really needs
        // Some Task could be very interesting, so we want to get them by biding less
        // So we calculate the expected marginal cost of next step and compare the winning and losing case

        Long estimatedCostIfWin = 0L, estimatedCostIfLost = 0L;

        Random random = new Random();

        // Randomly select 'nbPossibleTask' and compute the mean of marginal cost
        for (int i = 0 ; i < nbPossibleTask; i++){

            Task newTask = bestTask.get(random.nextInt(bestTaskSize));

            /*** Estimation of future reward if won ***********************************************/

            ArrayList<Task> futureIfTaskWon = new ArrayList<>(ifTaskWinned);
            futureIfTaskWon.add(newTask);
            Planner plannerWon = new Planner(this.vehicles, futureIfTaskWon);
            List<Plan> _1 = plannerWon.simulatedAnnealingSearch();
            estimatedCostIfWin += (long) Math.abs(plannerWon.getCost() - newCost);

            /*** Estimation of future reward if lost **********************************************/

            ArrayList<Task> futureIfTaskLost = new ArrayList<>(winnedTasks);
            futureIfTaskLost.add(newTask);
            Planner plannerLost = new Planner(this.vehicles, futureIfTaskLost);
            List<Plan> _2 = plannerLost.simulatedAnnealingSearch();
            estimatedCostIfLost += (long) Math.abs(plannerLost.getCost() - currentCost);
        }

        /*** Compute de difference between the to expected marginal cost **************************/

        Long estimationFuture =  (long)((estimatedCostIfWin - estimatedCostIfLost)/nbPossibleTask);
        //System.out.println("estimationFuture = " + estimationFuture);

        /*** Bid **********************************************************************************/

        // Minimum threshold at 'minimumBid'
        bid = (Long) Math.max(marginalCost + estimationFuture, minimumBid) ;

        /*** Check if the agent have a chance to win **********************************************/

        if (turn > 1){
            // Get the lowest probability of wining
            double p = 0, minP = Double.MAX_VALUE;
            for (int i = 0 ; i < nb_player ; i++){
                if (i == agent.id()){
                    continue;
                }else{
                    p = getProbaWinVS(bibsHistory.get(i), bid);
                    if (p < minP) minP = p;
                }
            }
            // If it is higher than 1/2 bid a little bit more
            if (p > 1/2){
                bid = (long)Math.ceil(1.3*bid);
            }else{
                // else bif a little bit less
                bid = (long)Math.ceil(0.9*bid);
            }
        }
        return bid;
    }

    /**********************************************************************************************
     *
     *  auctionResult
     *
     **********************************************************************************************/

    @Override
    public void auctionResult(Task previous, int winner, Long[] bids) {

        /*** Initialisation on turn 0 *************************************************************/

        if (turn == 0){
            nb_player = bids.length;
            bibsHistory = new ArrayList<>(nb_player);
            for (int i = 0 ; i < nb_player ; i++){
                bibsHistory.add(0, new LinkedList<>());
                bibsHistory.get(i).add(bids[i]);
            }
        }

        /*** Update of bid histories **************************************************************/

        for (int i = 0 ; i < nb_player ; i++){
            bibsHistory.get(i).add(bids[i]);
        }

        /*** Update of task history and total cost ************************************************/

        if (winner == this.agent.id()){
            this.winnedTasks.add(previous);
            currentCost = newCost;
            System.out.printf("Agent %d won task %d with reward %d %n", winner, previous.id, previous.reward);
        }

        turn++;
    }


    /**********************************************************************************************
     *
     *  getProbaWinVS
     *
     *  Return the probability of winning with bid "bid" again an opponent with bid history
     *  "bidHistory".
     *  The probability is modeled as a sigmoid centered on the mean and stretched by the variance.
     *
     **********************************************************************************************/

    public double getProbaWinVS(List<Long> bidHistory, Long bid) {

        Long E = 0L;
        Long Var = 0L;
        int size = 0;
        double p = 0;

        /*** Expectancy ***************************************************************************/

        for (Long bidH:  bidHistory){
            E += bidH;
            size += 1;
        }
        E = ((Long)E/size);

        /*** Variance *****************************************************************************/

        for (Long bidH:  bidHistory){
            Var += (bidH - E)*(bidH - E);
        }
        Var = ((Long)Var/size);

        /*** Sigmoid ******************************************************************************/
        // Maybe it can be improved

        p = 1/(1+Math.exp((E-bid)*Var));
        return 1-p;
    }

    /**********************************************************************************************
     *
     *  plan
     *
     **********************************************************************************************/

    @Override
    public List<Plan> plan(List<Vehicle> vehicles, TaskSet tasks) {
        Planner planner = new Planner(vehicles, tasks);
        List<Plan> plans = planner.simulatedAnnealingSearch();
        System.out.printf("This was the plan for agent %d with reward %d and cost %d %n %n", agent.id(), tasks.rewardSum(), planner.getCost());
        return plans;
    }

}
