let wakeLock = null;

const requestWakeLock = async () => {
    try {
        wakeLock = await navigator.wakeLock.request('screen');
        console.log('Wake Lock is active! Screen will not turn off.');

        wakeLock.addEventListener('release', () => {
            console.log('Wake Lock has been released');
        });
    } catch (err) {
        console.warn(`Wake Lock error: ${err.name}, ${err.message}`);
    }
};

document.addEventListener('DOMContentLoaded', () => {
    if ('wakeLock' in navigator) {
        requestWakeLock();
    } else {
        console.warn('Wake Lock API not supported in this browser.');
    }
});

document.addEventListener('visibilitychange', async () => {
    if (document.visibilityState === 'visible' && 'wakeLock' in navigator) {
        // Re-request the wake lock when the page becomes visible again
        requestWakeLock();
    }
});
