import scala.util.Random
import scala.concurrent.duration._
import scala.language.higherKinds

import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import cats.effect.concurrent.{MVar, Ref}


object WorkerPool extends IOApp {

  type Worker[F[_], A, B] = A => F[B]

  def mkWorker[F[_]](id: Int)(implicit timer: Timer[F], CE: Concurrent[F]): F[Worker[F, Int, Int]] =
    Ref[F].of(0).map { counter =>
      def simulateWork: F[Unit] =
        CE.delay(50 + Random.nextInt(450)).map(_.millis).flatMap(timer.sleep)

      def report: F[Unit] =
        counter.get.flatMap(i => CE.delay(println(s"Total processed by $id: $i")))

      x =>
        simulateWork >>
          counter.update(_ + 1) >>
          report >>
          CE.pure(x + 1)
    }

  trait WorkerPool[F[_], A, B] {
    def exec(a: A): F[B]
    def add(worker: Worker[F, A, B]): F[Unit]
    def removeAll: F[Unit]
  }

  object WorkerPool {

    def of[F[_] : Concurrent, A, B](fs: List[Worker[F, A, B]]): F[WorkerPool[F, A, B]] = for {
      reference <- Ref[F].of(fs)
      queue     <- MVar.empty[F, Worker[F, A, B]]
      _         <- fs.map(queue.put).map(_.start.void).sequence

      workerPool = new WorkerPool[F, A, B] {
        def exec(a: A): F[B] = for {
          worker <- queue.take
          b      <- worker.apply(a).guarantee(putBack(worker))
        } yield b

        def add(worker: Worker[F, A, B]): F[Unit] =
          reference.update(_ :+ worker) >> queue.put(worker).start.void

        def removeAll: F[Unit] =
          reference.set(List.empty)

        private def putBack(worker: Worker[F, A, B]): F[Unit] = for {
          contains <- reference.get.map(_.contains(worker))
          _        <- if (contains) queue.put(worker).start.void else Concurrent[F].unit
        } yield ()

      }
    } yield workerPool
  }

  val testPool: IO[WorkerPool[IO, Int, Int]] =
    List
      .range(0, 10)
      .traverse(mkWorker[IO])
      .flatMap(WorkerPool.of[IO, Int, Int])

  def run(args: List[String]): IO[ExitCode] =
    for {
      pool <- testPool
      _    <- pool.exec(42).replicateA(20)
    } yield ExitCode.Success
}
