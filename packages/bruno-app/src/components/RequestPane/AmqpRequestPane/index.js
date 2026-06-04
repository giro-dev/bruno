import React, { useMemo, useCallback, useRef } from 'react';
import Documentation from 'components/Documentation/index';
import RequestHeaders from 'components/RequestPane/RequestHeaders';
import StatusDot from 'components/StatusDot/index';
import { find } from 'lodash';
import { updateRequestPaneTab } from 'providers/ReduxStore/slices/tabs';
import { useDispatch, useSelector } from 'react-redux';
import HeightBoundContainer from 'ui/HeightBoundContainer';
import ResponsiveTabs from 'ui/ResponsiveTabs';
import { getPropertyFromDraftOrRequest } from 'utils/collections/index';
import StyledWrapper from './StyledWrapper';
import AmqpBody from './AmqpBody';
import AmqpSettings from './AmqpSettings';

const AmqpRequestPane = ({ item, collection, handleRun }) => {
  const dispatch = useDispatch();
  const tabs = useSelector((state) => state.tabs.tabs);
  const activeTabUid = useSelector((state) => state.tabs.activeTabUid);

  const focusedTab = find(tabs, (t) => t.uid === activeTabUid);
  const requestPaneTab = focusedTab?.requestPaneTab;

  const selectTab = useCallback(
    (tab) => {
      dispatch(updateRequestPaneTab({
        uid: item.uid,
        requestPaneTab: tab
      }));
    },
    [dispatch, item.uid]
  );

  const headers = getPropertyFromDraftOrRequest(item, 'request.headers');
  const docs = getPropertyFromDraftOrRequest(item, 'request.docs');
  const auth = getPropertyFromDraftOrRequest(item, 'request.auth');

  const activeHeadersLength = headers.filter((header) => header.enabled).length;

  const allTabs = useMemo(() => {
    return [
      {
        key: 'body',
        label: 'Message',
        indicator: null
      },
      {
        key: 'headers',
        label: 'Headers',
        indicator: activeHeadersLength > 0 ? <sup className="ml-[.125rem] font-medium">{activeHeadersLength}</sup> : null
      },
      {
        key: 'settings',
        label: 'Settings',
        indicator: null
      },
      {
        key: 'docs',
        label: 'Docs',
        indicator: docs && docs.length > 0 ? <StatusDot type="default" /> : null
      }
    ];
  }, [activeHeadersLength, auth?.mode, docs]);

  const tabPanel = useMemo(() => {
    switch (requestPaneTab) {
      case 'body': {
        return (
          <AmqpBody
            item={item}
            collection={collection}
            handleRun={handleRun}
          />
        );
      }
      case 'headers': {
        return <RequestHeaders item={item} collection={collection} addHeaderText="Add Headers" />;
      }
      case 'settings': {
        return <AmqpSettings item={item} collection={collection} />;
      }
      case 'docs': {
        return <Documentation item={item} collection={collection} />;
      }
      default: {
        return <div className="mt-4">404 | Not found</div>;
      }
    }
  }, [requestPaneTab, item, collection, handleRun]);

  if (!activeTabUid || !focusedTab?.uid || !requestPaneTab) {
    return <div className="pb-4 px-4">An error occurred!</div>;
  }

  return (
    <StyledWrapper className="flex flex-col h-full relative">
      <ResponsiveTabs
        tabs={allTabs}
        activeTab={requestPaneTab}
        onTabSelect={selectTab}
      />

      <section className="flex w-full flex-1 h-full mt-4">
        <HeightBoundContainer>{tabPanel}</HeightBoundContainer>
      </section>
    </StyledWrapper>
  );
};

export default AmqpRequestPane;
