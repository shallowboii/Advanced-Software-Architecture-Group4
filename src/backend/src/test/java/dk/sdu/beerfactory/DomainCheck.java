package dk.sdu.beerfactory;

/** Exhaustive admission/transfer guard checks; runnable without third-party dependencies. */
public final class DomainCheck {
    private static int checks;
    private static void check(boolean condition){checks++;if(!condition)throw new AssertionError("check "+checks);}
    public static void main(String[] args){
        for(String policy:new String[]{"PUSH","CONWIP"})for(int cap:new int[]{1,20,40})for(int wip=0;wip<=41;wip++)
            for(boolean stopped:new boolean[]{false,true})for(boolean free:new boolean[]{false,true})for(boolean stock:new boolean[]{false,true}){
                boolean allowed=Domain.canAdmit(policy,wip,cap,stopped,free,stock);
                check(!allowed||(!stopped&&free&&stock));
                check(policy.equals("PUSH")||!allowed||wip<cap);
                if(!stopped&&free&&stock)check(allowed==(policy.equals("PUSH")||wip<cap));
            }
        for(String state:new String[]{"PREPARING","EXECUTING","ABORTING","COMMITTED","DONE"})
            for(boolean source:new boolean[]{false,true})for(boolean target:new boolean[]{false,true})for(boolean loss:new boolean[]{false,true}){
                boolean commit=Domain.canCommitTransfer(source,target,state,loss);
                check(!commit||(source&&target&&!loss&&state.equals("EXECUTING")));
                if(source&&target&&!loss&&state.equals("EXECUTING"))check(commit);
            }
        check(Domain.validOwner("unitank-1","execution-current","unitank-1","execution-current"));
        check(!Domain.validOwner("unitank-1","execution-current","unitank-1","execution-previous"));
        check(!Domain.validOwner("unitank-1","execution-current","whirlpool-1","execution-current"));
        check(!Domain.validOwner("unitank-1","execution-current","unitank-1",null));
        for(int stage=0;stage<6;stage++)for(String status:new String[]{"PROCESSING","COMPLETED","DISCARDED"})for(boolean lost:new boolean[]{false,true})
            check(Domain.canComplete(stage,status,lost)==(stage==5&&status.equals("PROCESSING")&&!lost));
        for(double invalid:new double[]{Double.NaN,Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY,-1,0}){
            try{Domain.positiveFinite(invalid,"time");throw new AssertionError("accepted invalid time");}catch(IllegalArgumentException expected){checks++;}
        }
        System.out.println("Domain invariants: "+checks+" checks passed");
    }
}
