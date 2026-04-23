<?php

namespace Digbang\SafeQueue;

use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use Exception;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Container\Container;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\Worker as IlluminateWorker;
use Illuminate\Queue\WorkerOptions;
use Throwable;

class Worker extends IlluminateWorker
{
    /**
     * @var ManagerRegistry
     */
    protected $managerRegistry;

    /**
     * Worker constructor.
     *
     * @param QueueManager $manager
     * @param Dispatcher $events
     * @param ManagerRegistry $managerRegistry
     * @param ExceptionHandler $exceptions
     * @param \callable $isDownForMaintenance
     */
    public function __construct(
        QueueManager $manager,
        Dispatcher $events,
        ManagerRegistry $managerRegistry,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance
    ) {
        parent::__construct($manager, $events, $exceptions, $isDownForMaintenance);

        $this->managerRegistry = $managerRegistry;
    }

    /**
     * Wrap parent::getNextJob to make sure we have a good EM before processing the next job.
     * This allow us to avoid incrementing the attempts on the job if the worker fails because of the EM.
     *
     * Get the next job from the queue connection.
     *
     * @param  \Illuminate\Contracts\Queue\Queue  $connection
     * @param  string  $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    protected function getNextJob($connection, $queue)
    {
        $exception = null;

        try {
            $this->assertEntityManagerOpen();
            $this->assertEntityManagerClear();
            $this->assertGoodDatabaseConnection();
        } catch (EntityManagerClosedException $e) {
            $exception = $e;
        } catch (Exception $e) {
            $exception = new QueueSetupException("Error in queue setup while getting next job", 0, $e);
        } catch (Throwable $e) {
            $exception = new QueueSetupException("Error in queue setup while getting next job", 0, $e);
        }

        if ($exception) {
            $this->shouldQuit = true;
            $this->exceptions->report($exception);

            return null;
        }

        return parent::getNextJob($connection, $queue);
    }

    /**
     * @throws EntityManagerClosedException
     */
    private function assertEntityManagerOpen()
    {
        foreach ($this->managerRegistry->getManagers() as $entityManager) {
            if (!$entityManager->isOpen()) {
                throw new EntityManagerClosedException;
            }
        }
    }

    /**
     * To clear the em before doing any work.
     */
    private function assertEntityManagerClear()
    {
        foreach ($this->managerRegistry->getManagers() as $entityManager) {
            $entityManager->clear();
        }
    }

    /**
     * Some database systems close the connection after a period of time, in MySQL this is system variable
     * `wait_timeout`. Given the daemon is meant to run indefinitely we need to make sure we have an open
     * connection before working any job. Otherwise we would see `MySQL has gone away` type errors.
     */
    private function assertGoodDatabaseConnection()
    {
        foreach ($this->managerRegistry->getManagers() as $entityManager) {
            $connection = $entityManager->getConnection();

            try {
                $connection->executeQuery($connection->getDatabasePlatform()->getDummySelectSQL());
            } catch (Throwable) {
                $connection->close();
                $connection->connect();
            }
        }
    }
}
