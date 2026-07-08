// connection-status-stream.ts (Observable-like stream without rxjs dependency)
import { TAuthData } from '@/types/api-types';

export enum CONNECTION_STATUS {
    OPENED = 'opened',
    CLOSED = 'closed',
    UNKNOWN = 'unknown',
}

// Simple Subject-like implementation without rxjs
class SimpleSubject<T> {
    private value: T;
    private subscribers: Set<(value: T) => void> = new Set();

    constructor(initialValue: T) {
        this.value = initialValue;
    }

    next(value: T) {
        this.value = value;
        this.subscribers.forEach(subscriber => subscriber(value));
    }

    getValue() {
        return this.value;
    }

    subscribe(observer: (value: T) => void) {
        this.subscribers.add(observer);
        // Call immediately with current value
        observer(this.value);
        // Return unsubscribe function
        return () => {
            this.subscribers.delete(observer);
        };
    }
}

// Initial connection status will be 'unknown'
export const connectionStatus$ = new SimpleSubject<string>('unknown');
export const isAuthorizing$ = new SimpleSubject<boolean>(true); // Start with true to show loader immediately
export const isAuthorized$ = new SimpleSubject<boolean>(false);
export const account_list$ = new SimpleSubject<TAuthData['account_list']>([]);
export const authData$ = new SimpleSubject<TAuthData | null>(null);

// Create functions to easily update status
export const setConnectionStatus = (status: CONNECTION_STATUS) => {
    connectionStatus$.next(status);
};

// Set the authorized status
export const setIsAuthorized = (isAuthorized: boolean) => {
    isAuthorized$.next(isAuthorized);
};

// Set the authorizing status
export const setIsAuthorizing = (isAuthorizing: boolean) => {
    isAuthorizing$.next(isAuthorizing);
};

// Set the account list
export const setAccountList = (accountList: TAuthData['account_list']) => {
    account_list$.next(accountList);
};

// Set the auth data
export const setAuthData = (authData: TAuthData | null) => {
    if (authData?.loginid) {
        localStorage.setItem('active_loginid', authData.loginid);
    }
    authData$.next(authData);
};
