package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    runner(0,chars);
  }
  def runner(count:Int,chars: Array[Char]):Boolean={
    if(chars.isEmpty)
       count==0;
    else if(count<0)
       false;
    else if(chars.head.equals('('))
       runner(count+1,chars.tail);
    else if(chars.head.equals(')'))
       runner(count-1,chars.tail);
    else
       runner(count,chars.tail);
    
 }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int) :(Int,Int) = {
      
        def runner(chars: Array[Char], lcount: Int, rcount: Int):(Int,Int)={
          if(chars.isEmpty)
            (lcount,rcount);
          else if(chars.head.equals('('))
             runner(chars.tail,lcount+1,rcount);
          else if(chars.head.equals(')'))
              if(lcount==0)
                 runner(chars.tail,lcount,rcount+1);
              else
                 runner(chars.tail,lcount-1,rcount);
          else
             runner(chars.tail,lcount,rcount);
    
        }
        runner(chars.slice(idx, until),arg1,arg2);
    }

    def reduce(from: Int, until: Int) :(Int,Int) = {
      
      if(until-from<=threshold)
        traverse(from, until,0,0);
      else{ 
        val mid=(from+until)/2;
        val (r1,r2)=parallel(reduce(from,mid),
                  reduce(mid,until));
        
        if(r1._1-r2._2>=0)
          (r1._1-r2._2+r2._1,r1._2);
        else
          (r2._1,r2._2-r1._1+r1._2);
        
      }
      
    }

    reduce(0, chars.length)==(0,0);
   
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
