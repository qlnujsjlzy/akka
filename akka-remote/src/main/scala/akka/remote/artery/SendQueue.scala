/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import akka.stream.stage.GraphStage
import akka.stream.stage.OutHandler
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue
import akka.stream.stage.GraphStageWithMaterializedValue
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueueTail
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * INTERNAL API
 */
private[remote] object SendQueue {
  trait OfferApi[T] {
    def offer(message: T): Boolean
  }

  private trait WakeupSignal {
    def wakeup(): Unit
  }
}

/**
 * INTERNAL API
 */
private[remote] final class SendQueue[T](capacity: Int) extends GraphStageWithMaterializedValue[SourceShape[T], SendQueue.OfferApi[T]] {
  import SendQueue._

  val out: Outlet[T] = Outlet("SendQueue.out")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, OfferApi[T]) = {
    @volatile var needWakeup = false
    val queue = new ManyToOneConcurrentArrayQueue[T](capacity)
    // TODO perhaps try similar with ManyToOneConcurrentLinkedQueue or AbstractNodeQueue

    val logic = new GraphStageLogic(shape) with OutHandler with WakeupSignal {
      private val wakeupCallback = getAsyncCallback[Unit] { _ ⇒
        if (isAvailable(out))
          tryPush()
      }

      override def onPull(): Unit =
        tryPush()

      private def tryPush(): Unit = {
        needWakeup = true
        queue.poll() match {
          case null ⇒
          // empty queue, needWakeup is true
          // note that it is not enough to set needWakeup here
          case elem ⇒
            needWakeup = false // there will be another onPull
            push(out, elem)
        }
      }

      // external call
      override def wakeup(): Unit =
        wakeupCallback.invoke(())

      override def postStop(): Unit = {
        queue.clear()
        super.postStop()
      }

      setHandler(out, this)
    }

    val offerApi = new OfferApi[T] {
      override def offer(message: T): Boolean = {
        val result = queue.offer(message)
        if (result && needWakeup)
          logic.wakeup()
        result
      }
    }

    (logic, offerApi)

  }
}
