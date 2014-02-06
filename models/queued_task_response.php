<?php

/* QueuedTaskResponse

Usage:
 
In some controller:

var $uses = array('Queue.QueuedTask', 'Queue.QueuedTaskResponse');

function action() {
	$key = $this->QueuedTaskResponse->generate();
	$this->QueuedTask->createJob('some_task', array('response_key' => $key));
	$response = $this->QueuedTaskResponse->getValue($key, true);
	$this->set('response', $response);
}

In your queue task:

var $uses = array('Queue.QueuedTaskResponse');
function run($data) {
	$this->QueuedTaskResponse->setValue($data['response_key'], 'Return value');
}

*/

class InvalidQueuedTaskResponseKeyException extends Exception {}

class QueuedTaskResponse extends AppModel {
	
	var $useDbConfig = 'central';

	var $validate = array(
		'key' => array(
			'nonempty' => array (
				'rule' => 'notempty',
				'required' => true,
				'message' => 'A key must be provided',
				),
			'unique' => array (
				'rule' => 'isUnique',
				'required' => true,
				'message' => 'This key already exists',
				),
			));
	
	public $name = 'QueuedTaskResponse';

	public function generate() {
		$this->create();
		$key = null;
		while (!$this->validates(array('fieldList' => array('key'))))
		{
			$key = $this->_generateRandomString(32);
			$this->set('key', $key);
		}
		$this->save();

		return $key;
	}

	public function setValue($key, $value) {
		$response = $this->findByKey($key);
		if ($response == array())
			throw new InvalidQueuedTaskResponseKeyException('Key not found in QueuedTaskResponses');

		$response['QueuedTaskResponse']['value'] = serialize($value);

		return $this->save($response);
	}

	public function getValue($key, $block = false) {
		$value = null;

		if ($block) {
			try {
				// If we're blocking, loop until we get a non-null value
				while (is_null($value)) {
					$value = $this->_getValue($key);
					if(is_null($value))
						sleep(100);
				}
			} catch (InvalidQueuedTaskResponseKeyException $e) {
				return false;
			}
		} else { 
			// Not blocking, so try once and return whatever we get.
			try {
				$value = $this->_getValue($key);
			} catch (InvalidQueuedTaskResponseKeyException $e) {
				return false;
			}
		}

		$value = unserialize($value);
		return $value;
	}

	public function _getValue($key) {
		$response = $this->findByKey($key);
		if ($response == array())
			throw new InvalidQueuedTaskResponseKeyException('Key not found in QueuedTaskResponses');
		return $response['QueuedTaskResponse']['value'];
	}

	function _generateRandomString($length) {
		$alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890';
		$retval = '';
		for ($i = 0; $i < $length; $i++)
			$retval .= substr($alphabet, mt_rand(0, strlen($alphabet)-1), 1);

		return $retval;
	}
}
