package pku;

/**
 * @author ck
 * @create 2021-09-18-21:46
 */
public class Solution {
    public int findKthLargest(int[] nums, int k) {
        quicksort(nums, 0, nums.length - 1);
        return nums[nums.length-k];
    }
    public void quicksort(int[] nums, int start, int end){
        if(start >= end){
            return;
        }
        int p = partition(nums, start, end);
        quicksort(nums, start, p - 1);
        quicksort(nums, p + 1, end);
    }
    public int partition(int[] nums, int start, int end){
        int x = nums[end];
        int i = start - 1;
        int temp = 0;
        for(int j = start; j < end -1; j++){
            if(nums[j] <= x){
                i++;
                temp = nums[i];
                nums[i] = nums[j];
                nums[j] = temp;
            }
        }
        temp = nums[i + 1];
        nums[i + 1] = nums[end];
        nums[end] = temp;
        return i + 1;
    }

    public static void main(String[] args) {
//        Solution s = new Solution();
//        System.out.println(s.findKthLargest(new int[]{3,2,3,1,2,4,5,5,6},4));
        test2 ts = new test2();
        ts.pr();
    }
}

class SingleTon{
    private volatile SingleTon singleton = null;
    private SingleTon(){}
    public SingleTon getInstence(){
        if(singleton == null){
            synchronized (SingleTon.class){
                if(singleton == null){
                    singleton = new SingleTon();
                }
            }
        }
        return singleton;
    }
}

class test1{
    public int x = 1;
    public int pr(){
        System.out.println(x);
        return 1;
    }
}

class test2 extends test1{
    @Override
    public int pr(){
        super.pr();
        System.out.println(22);
        return 2;
    }
}