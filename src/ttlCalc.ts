export const calculateTTL = () => {
  const random = Math.random() * 20;
  if (random < 5) {
    return 5000;
  }
  if (random < 10) {
    return 10000;
  }
  if (random < 15) {
    return 15000;
  }
  if (random <= 20) {
    return 20000;
  }
  return 25000;
};
