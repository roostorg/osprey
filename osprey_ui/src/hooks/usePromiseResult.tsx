import * as React from 'react';

export enum PromiseResultStatus {
  Resolving,
  Resolved,
  Rejected,
}

type Resolver<T> = () => Promise<T>;
export type PromiseResult<T> =
  | { status: PromiseResultStatus.Resolving }
  | { status: PromiseResultStatus.Resolved; value: T }
  | { status: PromiseResultStatus.Rejected; error: Error; retry: () => void };

export default function usePromiseResult<T>(resolver: Resolver<T>, deps: React.DependencyList = []): PromiseResult<T> {
  const [state, setState] = React.useState<PromiseResult<T>>({ status: PromiseResultStatus.Resolving });
  const [retryCounter, setRetryCounter] = React.useState<number>(0);

  React.useEffect(
    () => {
      // Set to false when the effect un-mounts, so we know to discard the result of the
      // promise.
      let isCurrent = true;
      // Regardless of what the state when we start running the effect, we need to transition back
      // to the resolving state.
      setState({ status: PromiseResultStatus.Resolving });

      const resolveData = async () => {
        try {
          const value = await resolver();
          if (isCurrent) {
            setState({ status: PromiseResultStatus.Resolved, value });
          }
        } catch (error: any) {
          // eslint-disable-next-line no-console
          console.error('Error while resolving promise', error);
          if (isCurrent) {
            const retry = () => {
              // Check if we're current, and only retry if we are. If someone calls `retry`, but we have
              // unmounted, or we've re-mounted the effect, we can ignore this request to retry.
              if (isCurrent) {
                // Increment the retry counter, which will cause the effect to re-mount, and trigger
                // a new loading indicator.
                setRetryCounter((x) => x + 1);
              }
            };
            setState({ status: PromiseResultStatus.Rejected, error, retry });
          }
        }
      };

      resolveData();

      return () => {
        isCurrent = false;
      };
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [retryCounter, ...deps]
  );

  return state;
}
