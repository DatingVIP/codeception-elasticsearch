<?php

namespace Codeception\Module;

use Codeception\Module;
use Codeception\Lib\ModuleContainer;

use Elasticsearch\ClientBuilder;

class Elasticsearch extends Module
{
    public $elasticsearch;

    public function _initialize()
    {
        $clientBuilder = ClientBuilder::create();
        $clientBuilder->setHosts($this->_getConfig('hosts'));
        $this->client = $clientBuilder->build();

        if ($this->_getConfig('cleanup') !== true) {
            return;
        }

        $this->cleanData();

        //make dummy indexes (to avoid exceptions on checking empy index)
        $create = $this->_getConfig('create');
        if (!empty($create) && is_array($create)) {
            foreach ($create as $index) {
                $this->client->index(['index' => $index, 'body' => []]);
            }
        }
    }

    private function cleanData()
    {
        $selective = $this->_getConfig('selective');
        if (!empty($selective) && is_array($selective)) {
            foreach ($selective as $index) {
                $this->client->indices()->delete(['index' => $index]);
            }
            return;
        }

        $this->client->indices()->delete(['index' => '*']);
    }

    public function grabFromElasticsearch($index = null, $type = null, $queryString = '*')
    {
        $this->client->indices()->refresh();

        $result = $this->client->search(
            [
                'index' => $index,
                'type' => $type,
                'q' => $queryString,
                'size' => 1
            ]
        );

        return !empty($result['hits']['hits'])
            ? $result['hits']['hits'][0]['_source']
            : array();
    }

    public function grabAllFromElasticsearch($index = null, $type = null, $queryString = '')
    {
        $this->client->indices()->refresh();

        $params = [
                'index' => $index,
                'type' => $type,
                'size' => 1000
        ];
        if (!empty($queryString)) {
            $params['body'] = ['query' => ['match' => $queryString]];
        }
        $result = $this->client->search($params);

        return !empty($result['hits']['hits'])
            ? $result['hits']['hits']
            : array();
    }

    public function seeInElasticsearch($index, $type, $fieldsOrValue)
    {
        return $this->assertTrue($this->count($index, $type, $fieldsOrValue) > 0, 'item exists');
    }

    public function dontSeeInElasticsearch($index, $type, $fieldsOrValue)
    {
        return $this->assertTrue($this->count($index, $type, $fieldsOrValue) === 0,
            'item does not exist');
    }

    protected function count($index, $type, $fieldsOrValue)
    {
        $query = [];

        if (is_array($fieldsOrValue)) {
            $query['bool']['filter'] = array_map(function ($value, $key) {
                return ['match' => [$key => $value]];
            }, $fieldsOrValue, array_keys($fieldsOrValue));
        }
        else {
            $query['multi_match'] = [
                'query' => $fieldsOrValue,
                'fields' => '_all',
            ];
        }

        $params = [
            'index' => $index,
            'type' => $type,
            'size' => 1,
            'body' => ['query' => $query],
        ];

        $this->client->indices()->refresh();

        $result = $this->client->search($params);

        return (int) $result['hits']['total']['value'];
    }

    public function haveInElasticsearch($document)
    {
        $result = $this->client->index($document);

        $this->client->indices()->refresh();

        return $result;
    }

}
