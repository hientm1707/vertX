package vn.edu.hcmut.interfaces;

public class MinusCalculatorImpl implements Calculator{
  @Override
  public int calculate(int a, int b) throws InterruptedException {
    Thread.sleep(5000);
    return a-b;
  }
}
