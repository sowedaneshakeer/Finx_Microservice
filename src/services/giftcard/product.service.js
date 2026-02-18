const globetopper = require('../../providers/globetopper.provider');

const getAllProducts = async (filters) => {
  return await globetopper.getProducts(filters);
};

const getProductById = async (id) => {
  const products = await globetopper.getProducts({});

  const product = products.find(p =>
    p.BillerID == id ||
    (p.operator && p.operator.id == id)
  );

  if (!product) return null;

  return {
    id: product.BillerID,
    operatorId: product.operator?.id,
    name: product.name,
    description: product.description,
    priceRange: {
      min: parseFloat(product.min),
      max: parseFloat(String(product.max).replace(/,/g, '')),
      increment: parseFloat(product.increment),
      isRange: product.is_a_range
    },
    currency: product.currency,
    category: product.category,
    type: product.type,
    attributes: product.request_attributes?.map(attr => ({
      name: attr.name,
      label: attr.label,
      required: attr.required
    })) || [],
    redemptionInfo: product.additional_details?.find(d => d.value)?.value
  };
};

module.exports = { getAllProducts, getProductById };
