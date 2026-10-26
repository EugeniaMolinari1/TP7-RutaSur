/** @type {import('next').NextConfig} */
const nextConfig = {
  /* config options here */
  cacheComponents: true,
  experimental: {
    cpus: 1,
  },
  partialPrefetching: true,
};

export default nextConfig;
