const API_BASE = import.meta.env.VITE_API_BASE ?? '/site2';
const TOKEN_KEY = 'site2_access_token';

export function getToken() {
  return window.localStorage.getItem(TOKEN_KEY) || '';
}

export function setToken(token) {
  if (!token) {
    window.localStorage.removeItem(TOKEN_KEY);
    return;
  }
  window.localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken() {
  window.localStorage.removeItem(TOKEN_KEY);
}

async function request(path, options = {}) {
  const token = getToken();
  const headers = {
    'Content-Type': 'application/json',
    ...(options.headers || {})
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch (error) {
    payload = null;
  }

  if (!response.ok) {
    const error = new Error(payload?.detail || `Request failed (${response.status})`);
    error.status = response.status;
    throw error;
  }

  return payload;
}

export async function apiGet(path) {
  return request(path, { method: 'GET' });
}

export async function apiPost(path, body) {
  return request(path, {
    method: 'POST',
    body: JSON.stringify(body ?? {})
  });
}
