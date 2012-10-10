<?php
declare(ticks = 1);

/**
 * @author MGriesbach@gmail.com
 * @package QueuePlugin
 * @subpackage QueuePlugin.Shells
 * @license http://www.opensource.org/licenses/mit-license.php The MIT License
 * @link http://github.com/MSeven/cakephp_queue
 */
class QueueShell extends Shell {
	public $uses = array(
		'Queue.QueuedTask'
	);
	/**
	 * Codecomplete Hint
	 *
	 * @var QueuedTask
	 */
	public $QueuedTask;
	
	private $taskConf;

	protected $_verbose = false;

	private $exit;

	/**
	 * Overwrite shell initialize to dynamically load all Queue Related Tasks.
	 */
	public function initialize() {
		App::import('Folder');
		$this->_loadModels();
		
		foreach ($this->Dispatch->shellPaths as $path) {
			$folder = new Folder($path . DS . 'tasks');
			$this->tasks = array_merge($this->tasks, $folder->find('queue_.*\.php'));
		}
		// strip the extension fom the found task(file)s
		foreach ($this->tasks as &$task) {
			$task = basename($task, '.php');
		}
		
		//Config can be overwritten via local app config.
		Configure::load('queue');
		
		$conf = Configure::read('queue');
		if (!is_array($conf)) {
			$conf = array();
		}
		//merge with default configuration vars.
		Configure::write('queue', array_merge(array(
			'sleeptime' => 10,
			'gcprop' => 10,
			'defaultworkertimeout' => 120,
			'defaultworkerretries' => 4,
			'workermaxruntime' => 0,
			'cleanuptimeout' => 2000,
			'exitwhennothingtodo' => false
		), $conf));

		if(isset($this->params['-verbose'])) {
			$this->_verbose = true;
		}
	}

	/**
	 * Output some basic usage Info.
	 */
	public function help() {
		$this->out('CakePHP Queue Plugin:');
		$this->hr();
		$this->out('Information goes here.');
		$this->hr();
		$this->out('Usage: cake queue <command> <arg1> <arg2>...');
		$this->hr();
		$this->out('Commands:');
		$this->out('	queue help');
		$this->out('		shows this help message.', 2);
		$this->out('	queue add <taskname>');
		$this->out('		tries to call the cli `add()` function on a task.');
		$this->out('		tasks may or may not provide this functionality.', 2);
		$this->out('	cake queue runworker [--verbose]');
		$this->out('		run a queue worker, which will look for a pending task it can execute.');
		$this->out('		the worker will always try to find jobs matching its installed tasks.');
		$this->out('		see "Available tasks" below.', 2);
		$this->out('	queue stats');
		$this->out('		display some general statistics.', 2);
		$this->out('	queue clean');
		$this->out('		manually call cleanup function to delete task data of completed tasks.', 2);
		$this->out('Note:');
		$this->out('	<taskname> may either be the complete classname (eg. `queue_example`)');
		$this->out('	or the shorthand without the leading "queue_" (eg. `example`).', 2);
		$this->_listTasks();
	}

	/**
	 * Look for a Queue Task of hte passed name and try to call add() on it.
	 * A QueueTask may provide an add function to enable the user to create new jobs via commandline.
	 *
	 */
	public function add() {
		if (count($this->args) < 1) {
			$this->out('Usage:');
			$this->out('       cake queue add <taskname>', 2);
			$this->_listTasks();
		} else {
			if (in_array($this->args[0], $this->taskNames)) {
				$this->{$this->args[0]}->add();
			} elseif (in_array('queue_' . $this->args[0], $this->taskNames)) {
				$this->{'queue_' . $this->args[0]}->add();
			} else {
				$this->out('Error:');
				$this->out('       Task not found: ' . $this->args[0], 2);
				$this->_listTasks();
			}
		}
	}

	/**
	 * Run a QueueWorker loop.
	 * Runs a Queue Worker process which will try to find unassigned jobs in the queue
	 * which it may run and try to fetch and execute them.
	 */
	public function runworker() {
		// Enable Garbage Collector (PHP >= 5.3)
		if (function_exists('gc_enable')) {
		    gc_enable();
		}
		pcntl_signal(SIGTERM, array(&$this, "_exit"));
		$this->exit = false;
		$starttime = time();
		$group = null;
		if (isset($this->params['group']) && !empty($this->params['group'])) {
			$group = $this->params['group'];
		}
		while (!$this->exit) {
			if($this->_verbose) {
				$this->out('Looking for Job....');
			}
			$data = $this->QueuedTask->requestJob($this->getTaskConf(), $group);
			if ($this->QueuedTask->exit === true) {
				$this->exit = true;
			} else {
				if ($data !== false) {
					$this->out('Running Job of type "' . $data['jobtype'] . '"');
					$taskname = 'queue_' . strtolower($data['jobtype']);
					$return = $this->{$taskname}->run(unserialize($data['data']));
					if ($return == true) {
						$this->QueuedTask->markJobDone($data['id']);
						$this->out('Job Finished.');
					} else {
						$failureMessage = null;
						if (isset($this->{$taskname}->failureMessage) && !empty($this->{$taskname}->failureMessage)) {
							$failureMessage = $this->{$taskname}->failureMessage;
						}
						$this->QueuedTask->markJobFailed($data['id'], $failureMessage);
						$this->out('Job did not finish, requeued.');
					}
				} elseif (Configure::read('queue.exitwhennothingtodo')) {
					$this->out('nothing to do, exiting.');
					$this->exit = true;
				} else {
					if($this->_verbose) {
						$this->out('nothing to do, sleeping.');
					}
					sleep(Configure::read('queue.sleeptime'));
				}
				
				// check if we are over the maximum runtime and end processing if so.
				if (Configure::read('queue.workermaxruntime') != 0 && (time() - $starttime) >= Configure::read('queue.workermaxruntime')) {
					$this->exit = true;
					$this->out('Reached runtime of ' . (time() - $starttime) . ' Seconds (Max ' . Configure::read('queue.workermaxruntime') . '), terminating.');
				}
				if ($this->exit || rand(0, 100) > (100 - Configure::read('queue.gcprop'))) {
					$this->out('Performing Old job cleanup.');
					$this->QueuedTask->cleanOldJobs();
				}
				if($this->_verbose) {
					$this->hr();
				}
			}
		}
	}

	/**
	 * Manually trigger a Finished job cleanup.
	 * @return null
	 */
	public function clean() {
		$this->out('Deleting old jobs, that have finished before ' . date('Y-m-d H:i:s', time() - Configure::read('queue.cleanuptimeout')));
		$this->QueuedTask->cleanOldJobs();
	}

	/**
	 * Display Some statistics about Finished Jobs.
	 * @return null
	 */
	public function stats() {
		$this->out('Jobs currenty in the Queue:');
		
		$types = $this->QueuedTask->getTypes();
		
		foreach ($types as $type) {
			$this->out("      " . str_pad($type, 20, ' ', STR_PAD_RIGHT) . ": " . $this->QueuedTask->getLength($type));
		}
		$this->hr();
		$this->out('Total unfinished Jobs      : ' . $this->QueuedTask->getLength());
		$this->hr();
		$this->out('Finished Job Statistics:');
		$data = $this->QueuedTask->getStats();
		foreach ($data as $item) {
			$this->out(" " . $item['QueuedTask']['jobtype'] . ": ");
			$this->out("   Finished Jobs in Database: " . $item[0]['num']);
			$this->out("   Average Job existence    : " . $item[0]['alltime'] . 's');
			$this->out("   Average Execution delay  : " . $item[0]['fetchdelay'] . 's');
			$this->out("   Average Execution time   : " . $item[0]['runtime'] . 's');
		}
	}

	/**
	 * Returns a List of available QueueTasks and their individual configurations.
	 * @return array
	 */
	private function getTaskConf() {
		if (!is_array($this->taskConf)) {
			$this->taskConf = array();
			foreach ($this->tasks as $task) {
				$this->taskConf[$task]['name'] = $task;
				if (property_exists($this->{$task}, 'timeout')) {
					$this->taskConf[$task]['timeout'] = $this->{$task}->timeout;
				} else {
					$this->taskConf[$task]['timeout'] = Configure::read('queue.defaultworkertimeout');
				}
				if (property_exists($this->{$task}, 'retries')) {
					$this->taskConf[$task]['retries'] = $this->{$task}->retries;
				} else {
					$this->taskConf[$task]['retries'] = Configure::read('queue.defaultworkerretries');
				}
				if (property_exists($this->{$task}, 'rate')) {
					$this->taskConf[$task]['rate'] = $this->{$task}->rate;
				}
			}
		}
		return $this->taskConf;
	}
/**
 * Output a list of available tasks.
 */
	protected function _listTasks() {
		$this->out('Available tasks:');
		foreach ($this->taskNames as $loadedTask) {
			$this->out('	- ' . $loadedTask);
		}
	}
	
	function out($str='') {
		$str = date('Y-m-d H:i:s').' '.$str;
		return parent::out($str);
	}

	function _exit($signal) {
		$this->exit = true;
	}

}
?>
