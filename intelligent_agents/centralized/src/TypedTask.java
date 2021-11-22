import logist.task.Task;

public class TypedTask{

    private Task task;
    private boolean type;   // 0 := pickup
                            // 1 := deliver

    public TypedTask(Task t,boolean type){
        this.task = t;
        this.type = type;
    }

    public boolean isDeliveringTask(){
        return type;
    }

    public boolean isPickupTask(){
        return !type;
    }
}
