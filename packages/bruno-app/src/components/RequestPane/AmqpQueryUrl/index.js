import { IconDeviceFloppy, IconPlugConnected, IconPlugConnectedX } from '@tabler/icons';
import SendButton from 'components/RequestPane/SendButton';
import classnames from 'classnames';
import SingleLineEditor from 'components/SingleLineEditor/index';
import { requestUrlChanged } from 'providers/ReduxStore/slices/collections';
import { saveRequest } from 'providers/ReduxStore/slices/collections/actions';
import { useTheme } from 'providers/Theme';
import React, { useEffect, useState, useMemo } from 'react';
import toast from 'react-hot-toast';
import { useDispatch } from 'react-redux';
import { isMacOS } from 'utils/common/platform';
import { hasRequestChanges } from 'utils/collections';
import StyledWrapper from './StyledWrapper';
import get from 'lodash/get';

const CONNECTION_STATUS = {
  CONNECTING: 'connecting',
  CONNECTED: 'connected',
  DISCONNECTED: 'disconnected'
};

const AmqpQueryUrl = ({ item, collection, handleRun }) => {
  const dispatch = useDispatch();
  const { theme, displayedTheme } = useTheme();
  const saveShortcut = isMacOS() ? '⌘S' : 'Ctrl+S';
  const hasChanges = useMemo(() => hasRequestChanges(item), [item]);

  const [connectionStatus, setConnectionStatus] = useState(CONNECTION_STATUS.DISCONNECTED);
  const url = item.draft ? get(item, 'draft.request.url', '') : get(item, 'request.url', '');

  const handleConnect = async () => {
    try {
      setConnectionStatus(CONNECTION_STATUS.CONNECTING);
      const { ipcRenderer } = window;
      const result = await ipcRenderer.invoke('renderer:amqp:connect', {
        request: item.draft ? item.draft.request : item.request,
        collection,
        environment: collection.activeEnvironmentUid,
        runtimeVariables: collection.runtimeVariables || {},
        settings: item.settings?.settings || {}
      });
      if (result.success) {
        setConnectionStatus(CONNECTION_STATUS.CONNECTED);
        toast.success('AMQP connected');
      } else {
        setConnectionStatus(CONNECTION_STATUS.DISCONNECTED);
        toast.error(`AMQP connection failed: ${result.error}`);
      }
    } catch (err) {
      setConnectionStatus(CONNECTION_STATUS.DISCONNECTED);
      toast.error(`AMQP connection error: ${err.message}`);
    }
  };

  const handleDisconnect = async (e, notify) => {
    e && e.stopPropagation();
    try {
      const { ipcRenderer } = window;
      await ipcRenderer.invoke('renderer:amqp:disconnect', {
        requestUid: item.uid,
        collectionUid: collection.uid
      });
      setConnectionStatus(CONNECTION_STATUS.DISCONNECTED);
      notify && toast.success('AMQP disconnected');
    } catch (err) {
      console.error('Failed to disconnect AMQP:', err);
      notify && toast.error('Failed to disconnect AMQP');
    }
  };

  const handleRunClick = async (e) => {
    e.stopPropagation();
    if (!url) {
      toast.error('Please enter a valid AMQP URL');
      return;
    }
    handleRun(e);
  };

  const onSave = () => {
    dispatch(saveRequest(item.uid, collection.uid));
  };

  const handleUrlChange = (value) => {
    const finalUrl = value?.trim() ?? value;
    dispatch(requestUrlChanged({
      itemUid: item.uid,
      collectionUid: collection.uid,
      url: finalUrl
    }));
  };

  // Listen for AMQP events from main process
  useEffect(() => {
    const { ipcRenderer } = window;
    const handleConnected = (event, requestUid, collectionUid) => {
      if (requestUid === item.uid && collectionUid === collection.uid) {
        setConnectionStatus(CONNECTION_STATUS.CONNECTED);
      }
    };
    const handleDisconnected = (event, requestUid, collectionUid) => {
      if (requestUid === item.uid && collectionUid === collection.uid) {
        setConnectionStatus(CONNECTION_STATUS.DISCONNECTED);
      }
    };
    const handleError = (event, requestUid, collectionUid, data) => {
      if (requestUid === item.uid && collectionUid === collection.uid) {
        toast.error(`AMQP error: ${data?.message || 'Unknown error'}`);
      }
    };

    const removeConnected = ipcRenderer.on('main:amqp:connected', handleConnected);
    const removeDisconnected = ipcRenderer.on('main:amqp:disconnected', handleDisconnected);
    const removeError = ipcRenderer.on('main:amqp:error', handleError);

    return () => {
      removeConnected();
      removeDisconnected();
      removeError();
    };
  }, [item.uid, collection.uid]);

  return (
    <StyledWrapper>
      <div className="flex items-center h-full">
        <div className="flex items-center input-container flex-1 min-w-0 h-full relative">
          <div className="flex items-center justify-center px-[10px]">
            <span className="text-xs font-medium method-amqp">AMQP</span>
          </div>
          <SingleLineEditor
            value={url}
            onSave={onSave}
            onChange={handleUrlChange}
            placeholder="amqp://localhost:5672"
            className="w-full"
            theme={displayedTheme}
            onRun={handleRun}
            collection={collection}
            item={item}
          />
          <div className="flex items-center h-full cursor-pointer gap-3 mx-3">
            <div
              className="infotip"
              onClick={(e) => {
                e.stopPropagation();
                if (!hasChanges) return;
                onSave();
              }}
            >
              <IconDeviceFloppy
                color={hasChanges ? theme.draftColor : theme.requestTabs.icon.color}
                strokeWidth={1.5}
                size={20}
                className={`${hasChanges ? 'cursor-pointer' : 'cursor-default'}`}
              />
              <span className="infotip-text text-xs">
                Save <span className="shortcut">({saveShortcut})</span>
              </span>
            </div>

            {connectionStatus === 'connected' && (
              <div className="connection-controls relative flex items-center h-full">
                <div className="infotip" onClick={(e) => handleDisconnect(e, true)}>
                  <IconPlugConnectedX
                    color={theme.colors.text.danger}
                    strokeWidth={1.5}
                    size={20}
                    className="cursor-pointer"
                  />
                  <span className="infotip-text text-xs">Disconnect</span>
                </div>
              </div>
            )}

            {connectionStatus !== 'connected' && (
              <div className="connection-controls relative flex items-center h-full">
                <div className="infotip" onClick={handleConnect}>
                  <IconPlugConnected
                    className={classnames('cursor-pointer', {
                      'animate-pulse': connectionStatus === CONNECTION_STATUS.CONNECTING
                    })}
                    color={theme.colors.text.green}
                    strokeWidth={1.5}
                    size={20}
                  />
                  <span className="infotip-text text-xs">Connect</span>
                </div>
              </div>
            )}
          </div>
          {connectionStatus === CONNECTION_STATUS.CONNECTED && <div className="connection-status-strip"></div>}
        </div>
        <SendButton
          onSend={handleRunClick}
          testId="run-button"
        />
      </div>
    </StyledWrapper>
  );
};

export default AmqpQueryUrl;
