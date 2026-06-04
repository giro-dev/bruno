import React from 'react';
import get from 'lodash/get';
import { useDispatch } from 'react-redux';
import { updateRequestBody } from 'providers/ReduxStore/slices/collections';
import { getPropertyFromDraftOrRequest } from 'utils/collections/index';
import CodeEditor from 'components/CodeEditor/index';
import { useTheme } from 'providers/Theme';

const AmqpBody = ({ item, collection }) => {
  const dispatch = useDispatch();
  const { displayedTheme } = useTheme();

  const body = getPropertyFromDraftOrRequest(item, 'request.body');
  const exchange = getPropertyFromDraftOrRequest(item, 'request.exchange') || '';
  const exchangeType = getPropertyFromDraftOrRequest(item, 'request.exchangeType') || 'direct';
  const routingKey = getPropertyFromDraftOrRequest(item, 'request.routingKey') || '';
  const queue = getPropertyFromDraftOrRequest(item, 'request.queue') || '';

  const amqpMessages = body?.amqp || [];
  const content = amqpMessages.length > 0 ? amqpMessages[0].content : '{}';

  const handleBodyChange = (value) => {
    dispatch(
      updateRequestBody({
        itemUid: item.uid,
        collectionUid: collection.uid,
        content: {
          mode: 'amqp',
          amqp: [
            {
              name: amqpMessages[0]?.name || 'message 1',
              type: amqpMessages[0]?.type || 'json',
              content: value
            }
          ]
        }
      })
    );
  };

  const handleFieldChange = (field, value) => {
    const { ipcRenderer } = window;
    // Use a custom action to update AMQP-specific fields
    dispatch({
      type: 'collections/updateAmqpRequestField',
      payload: {
        itemUid: item.uid,
        collectionUid: collection.uid,
        field,
        value
      }
    });
  };

  return (
    <div className="px-4 w-full h-full flex flex-col">
      <div className="grid grid-cols-2 gap-3 mb-4">
        <div>
          <label className="block text-xs font-medium mb-1 opacity-70">Exchange</label>
          <input
            type="text"
            className="w-full px-2 py-1 text-sm border rounded"
            value={exchange}
            onChange={(e) => handleFieldChange('exchange', e.target.value)}
            placeholder="my-exchange (optional)"
          />
        </div>
        <div>
          <label className="block text-xs font-medium mb-1 opacity-70">Exchange Type</label>
          <select
            className="w-full px-2 py-1 text-sm border rounded"
            value={exchangeType}
            onChange={(e) => handleFieldChange('exchangeType', e.target.value)}
          >
            <option value="direct">direct</option>
            <option value="topic">topic</option>
            <option value="fanout">fanout</option>
            <option value="headers">headers</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium mb-1 opacity-70">Routing Key</label>
          <input
            type="text"
            className="w-full px-2 py-1 text-sm border rounded"
            value={routingKey}
            onChange={(e) => handleFieldChange('routingKey', e.target.value)}
            placeholder="my.routing.key"
          />
        </div>
        <div>
          <label className="block text-xs font-medium mb-1 opacity-70">Queue</label>
          <input
            type="text"
            className="w-full px-2 py-1 text-sm border rounded"
            value={queue}
            onChange={(e) => handleFieldChange('queue', e.target.value)}
            placeholder="my-queue"
          />
        </div>
      </div>

      <label className="block text-xs font-medium mb-1 opacity-70">Message Body</label>
      <div className="flex-1 min-h-[200px]">
        <CodeEditor
          collection={collection}
          theme={displayedTheme}
          value={content || ''}
          onChange={handleBodyChange}
          mode="javascript"
        />
      </div>
    </div>
  );
};

export default AmqpBody;
