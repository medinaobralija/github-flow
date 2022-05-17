const get = require('lodash.get');
const { DateTime, Interval } = require('luxon');
const { parseGid } = require('@shopify/admin-graphql-api-utilities');

// Utils
const DB = require('../utils/database-util');
const RedisUtil = require('../utils/redis-util');

// Services
const socketService = require('../services/socket-service');

const {
	checkIfTrackIsValid,
	fetchCurrentROTM,
	addClubActiveMemberTag,
	validateSubscriptionType,
	validateCouponCode,
	normalizeSubscriptions,
	getCurrentTS,
	// dateIsBetweenTheLastDaysOfTheMonth
} = require('../helpers');

function subscriptionsController(shopifyService, chargebeeService) {
	// Update cancelled subscription inventory both on our DB and Shopify
	// If product is swapped: swap +1
	// If product in not swapped: existingsub -1 newsub +1
	async function updateCancelledSubscriptionInventory(obj) {
		const { connection, currentRotmId, productId, isSwappedProduct } = obj;
		const ssql = isSwappedProduct
			? 'available_swap_quantity = available_swap_quantity + 1'
			: `available_existingsub_quantity = available_existingsub_quantity - 1,
            available_newsub_quantity = available_newsub_quantity + 1`;

		// UPDATE DB
		await connection.query(
			`UPDATE rotms_swap_products
                SET ${ssql}, updated_at = current_timestamp
                WHERE rotm = ? AND product_id = ? AND status = 1`,
			[currentRotmId, productId]
		);

		// GET Shopify swap variant ID
		const variants = await shopifyService.fetchShopifyProductVariants(
			productId
		);

		// Validate
		if (!variants || variants.length === 0) return;

		// Shopify inventory: swap +1
		if (isSwappedProduct) {
			const swapVariantObj = variants.find(
				(item) => item.title === 'swap'
			);

			// Validate
			if (!swapVariantObj || !swapVariantObj.id) return;

			// Add to Queue -> Adjust variant's inventory
			// swap +1
			const VMPQueue = new RedisUtil.VMPQueue();
			await VMPQueue.addToQueue('adjust_variant_inventory_job', {
				variantId: parseInt(parseGid(swapVariantObj.id), 10),
				availableAdjustment: 1,
			});
		}

		// Shopify inventory: existingsub -1, newsub +1
		if (!isSwappedProduct) {
			const existingSubVariantObj = variants.find(
				(item) => item.title === 'existingsub'
			);
			const newSubVariantObj = variants.find(
				(item) => item.title === 'newsub'
			);

			// Validate
			if (!existingSubVariantObj || !newSubVariantObj) return;

			// Add to Queue -> Adjust variant's inventory
			// existingsub -1
			const VMPQueue = new RedisUtil.VMPQueue();
			await VMPQueue.addToQueue('adjust_variant_inventory_job', {
				variantId: parseInt(parseGid(existingSubVariantObj.id), 10),
				availableAdjustment: -1,
			});

			// newsub +1
			await VMPQueue.addToQueue('adjust_variant_inventory_job', {
				variantId: parseInt(parseGid(newSubVariantObj.id), 10),
				availableAdjustment: 1,
			});
		}
	}

	// Fetch all customer's subscriptions
	async function getSubscriptions(req, res, next) {
		try {
			const { customer_id: cbCustomerId } = req.query;

			// Validate
			if (!cbCustomerId) {
				const err = new Error('Missing params on get subscriptions.');
				err.status = 422;
				throw err;
			}

			// Return obj
			const respObj = {};

			const subscriptions =
				await chargebeeService.getCBCustomerSubscriptions(cbCustomerId);

			// Normalize
			const {
				addons,
				primarySubscription,
				primarySubId,
				primarySubPlanId,
				primarySubStatus,
			} = normalizeSubscriptions(subscriptions);

			if (addons) {
				respObj.cb_addons = addons;
			}

			if (primarySubscription) {
				respObj.cb_primary_subscription = primarySubscription;

				// GET Chargebee scheduled changes
				if (primarySubscription.has_scheduled_changes) {
					const { subscription: subWithChanges } =
						await chargebeeService.getCBSubscriptionWithChanges(
							primarySubId
						);

					if (subWithChanges.plan_id !== primarySubPlanId) {
						respObj.cb_has_term_changes = true;
					}

					let scBillingPeriod = get(subWithChanges, 'billing_period');
					let scBillingPeriodUnit = get(
						subWithChanges,
						'billing_period_unit'
					);

					if (
						scBillingPeriod === 1 &&
						scBillingPeriodUnit === 'year'
					) {
						scBillingPeriod = 12;
						scBillingPeriodUnit = 'month';
					}

					respObj.cb_subscription_with_changes = {
						billing_period: scBillingPeriod,
						billing_period_unit: scBillingPeriodUnit,
					};
				}
				// End of GET Chargebee scheduled changes

				// GET Chargebee renewal estimate
				if (primarySubStatus === 'active') {
					const estimate =
						await chargebeeService.getCBSubscriptionRenewalEstimate(
							primarySubId
						);

					if (estimate) {
						respObj.cb_upcoming_estimate = {
							sub_total: get(
								estimate,
								'invoice_estimate.sub_total'
							),
							total: get(estimate, 'invoice_estimate.total'),
						};
					}
				}
				// End of GET Chargebee renewal estimate
			}

			return res.status(200).send(respObj);
		} catch (error) {
			return next(error);
		}
	}

	// Create subscription for customer
	// - works for primary and addon subscriptions
	// - primary subscriptions may consist addons of separate tracks
	async function createSubscription(req, res, next) {
		try {
			const {
				type,
				track: postedTrack,
				selected_addons: selectedAddons,
				sh_customer_id: shCustomerId,
				cb_customer_id: cbCustomerId,
				plan_id: planId,
				product_id: productId,
				variant_id: variantId,
				coupon_code: couponCode,
				isOfferActive,
				// swap_window: swapWindow
			} = req.body;

			// Validate
			if (
				!shCustomerId ||
				!cbCustomerId ||
				!productId ||
				!variantId ||
				!postedTrack
			) {
				const err = new Error(
					'Missing params on create customer subscription.'
				);
				err.status = 422;
				throw err;
			}

			// Validate type
			const isValidType = validateSubscriptionType(type);

			if (!isValidType) {
				const err = new Error(
					'Invalid subscription type on create customer subscription.'
				);
				err.status = 422;
				throw err;
			}

			// Validate plan ID -> only on primary subscription
			// Addon plan ID will be calculated later
			if (type === 'primary' && !planId) {
				const err = new Error(
					'Missing plan on create customer subscription.'
				);
				err.status = 422;
				throw err;
			}

			// Connect to DB
			const pool = DB.getInstance();
			const connection = await pool.getConnection();

			// Begin Transaction
			await connection.beginTransaction();

			try {
				// Validate track
				const track = await checkIfTrackIsValid({
					connection,
					track: postedTrack,
				});

				if (!track) {
					const err = new Error(
						'Invalid track on create customer subscription.'
					);
					err.status = 422;
					throw err;
				}

				// Exclude CHARGEBEE_GIFT_COUPON_ID and PROMO coupons
				if (!validateCouponCode(couponCode)) {
					const err = new Error(
						"Sorry we couldn't find this discount code."
					);
					err.status = 422;
					throw err;
				}

				// Validate customer and email address
				const shCustomer =
					await shopifyService.fetchShopifyCustomerById(
						shCustomerId,
						true,
						true
					);

				if (!shCustomer) {
					const err = new Error(
						'Please sign up or login with your account.'
					);
					err.status = 422;
					throw err;
				}

				if (
					!shCustomer.defaultAddress ||
					!shCustomer.defaultAddress.last_name ||
					!shCustomer.defaultAddress.address1 ||
					!shCustomer.defaultAddress.city ||
					!shCustomer.defaultAddress.zip ||
					!shCustomer.defaultAddress.country ||
					!shCustomer.defaultAddress.phone
				) {
					const err = new Error(
						'Please enter a valid address/phone number.'
					);
					err.status = 422;
					throw err;
				}

				// Check inventory
				/* const isAvailable = await shopifyService.checkIfVariantIsAvailable(variantId);

                if (!isAvailable) {
                    const err = new Error('Sorry, this product is no longer available.');
                    err.status = 422;
                    throw err;
                } */

				// return res.status(200).send({ success: true });

				// GET all existing subscriptions
				const cbSubscriptions =
					await chargebeeService.getCBCustomerSubscriptions(
						cbCustomerId
					);

				// Validate for existing subscriptions
				if (cbSubscriptions && cbSubscriptions.length > 0) {
					// In case of primary
					if (type === 'primary') {
						// Search for same type primary subscription
						const primarySubscription = cbSubscriptions.find(
							(f) =>
								// eslint-disable-next-line implicit-arrow-linebreak
								f.subscription &&
								['active', 'future', 'non_renewing'].includes(
									f.subscription.status
								) &&
								f.subscription.meta_data &&
								f.subscription.meta_data.type &&
								f.subscription.meta_data.type === 'primary'
						);

						if (primarySubscription) {
							const err = new Error(
								'Subscription already added.'
							);
							err.status = 422;
							throw err;
						}
					}

					// In case of addon
					if (type === 'addon') {
						// Search for future same type and track addon subscription
						const addonSubscription = cbSubscriptions.find(
							(f) =>
								// eslint-disable-next-line implicit-arrow-linebreak
								f.subscription &&
								f.subscription.status === 'future' &&
								f.subscription.meta_data &&
								f.subscription.meta_data.type &&
								f.subscription.meta_data.type === 'addon' &&
								f.subscription.meta_data.product &&
								f.subscription.meta_data.product.track &&
								f.subscription.meta_data.product.track.includes(
									track
								)
						);

						if (addonSubscription) {
							const err = new Error(
								'This Add-On Track is already added.'
							);
							err.status = 422;
							throw err;
						}
					}
				}

				// In case of primary -> Validate address and plan
				if (type === 'primary') {
					const defaultAddressCountryName = get(
						shCustomer,
						'defaultAddress.country',
						''
					);

					if (
						defaultAddressCountryName === 'United States' &&
						planId.includes('-international')
					) {
						const err = new Error(
							'You chose an international plan but have a domestic Shipping Address. Please change your plan.'
						);
						err.status = 422;
						throw err;
					}

					if (
						defaultAddressCountryName !== 'United States' &&
						planId.includes('-usa')
					) {
						const err = new Error(
							'You chose a domestic plan but have an international Shipping Address. Please change your plan.'
						);
						err.status = 422;
						throw err;
					}
				}

				// Find addon plans:
				// 1-month-international-new
				// 1-month-usa-new
				// GET all CB plans
				const { list: addonPlans } =
					await chargebeeService.getCBAddonPlans();

				// Validate CB addon plans
				if (!addonPlans || addonPlans.length === 0) {
					const err = new Error('No Addon CB plans!');
					err.status = 500;
					throw err;
				}

				// USA Addon plan
				const usaAddonPlan = addonPlans.find((item) => {
					const addonPlanType = get(
						item,
						'plan.meta_data.plan_type',
						false
					);
					return addonPlanType === 'usa';
				});

				// International Addon plan
				const INAddonPlan = addonPlans.find((item) => {
					const addonPlanType = get(
						item,
						'plan.meta_data.plan_type',
						false
					);
					return addonPlanType === 'international';
				});

				// Validate addon plans
				if (!usaAddonPlan || !INAddonPlan) {
					const err = new Error('Addon CB plans not found!');
					err.status = 500;
					throw err;
				}

				const usaAddonPlanId = get(usaAddonPlan, 'plan.id', false);
				const INAddonPlanId = get(INAddonPlan, 'plan.id', false);

				// Find addon plan based on country code
				const countryCode = get(
					shCustomer,
					'defaultAddress.country_code',
					''
				);
				const addonPlanId =
					countryCode === 'US' ? usaAddonPlanId : INAddonPlanId;

				// SELECT current ROTM
				const { swapWindow, rotmRecords, currentRotmId } =
					await fetchCurrentROTM({ connection });

				// New CB Subscription POST object
				const pObj = {
					plan_id: type === 'primary' ? planId : addonPlanId,
					// auto_collection: 'on'
				};

				// Add coupon code -> if any
				if (couponCode && couponCode.trim()) {
					pObj.coupon_ids = [couponCode.trim()];
				}

				// Calculate startDate
				const now = DateTime.local().setZone('America/Denver');
				const firstDayOfNextMonth = Math.floor(
					now
						.endOf('month')
						.plus({ days: 1 })
						.startOf('day')
						.toSeconds()
				);

				/* if (dateIsBetweenTheLastDaysOfTheMonth(now, swapWindow)) {
                    startDate = Math.floor(now.endOf('month').plus({ days: 1 })
                        .startOf('day').toSeconds());
                } */

				// In case of addon or opened swapped window -> get the 1st day of next month
				if (type === 'addon' || swapWindow === 'opened') {
					pObj.start_date = firstDayOfNextMonth;
				}

				// Add meta_data
				pObj.meta_data = {
					swap_window: swapWindow,
					new: false,
					type,
					product: {
						id: productId,
						variant_id: variantId,
						track,
						swapped: false,
						swapped_date: null,
					},
				};

				const addonSubsPostObjs = [];
				if (type === 'primary') {
					// If the type is primary and the product id is not one of the upcoming ->
					// start with x (swx) -> subscriber gets the record he selected
					const isOnTheUpcoming = rotmRecords.some(
						(f) => f.product_id === parseInt(productId, 10)
					);

					if (!isOnTheUpcoming) {
						pObj.meta_data.swx = true;
						pObj.meta_data.product.swapped = true;
					}

					// Store product and variant ids of primary subscription's addons -> if any
					if (selectedAddons && selectedAddons.length > 0) {
						// Addon subscription POST objects
						selectedAddons.forEach((addonTrack) => {
							let addonTrackNormalized = addonTrack;

							if (addonTrack.includes('rap')) {
								addonTrackNormalized = 'hiphop';
							}

							const aPObj = {
								plan_id: addonPlanId,
								meta_data: {
									swap_window: swapWindow,
									new: false,
									type: 'addon',
									product: {
										track: addonTrackNormalized,
										swapped: false,
										swapped_date: null,
									},
								},
							};

							// In case of opened swapped window
							// -> get the 1st day of next month
							if (swapWindow === 'opened') {
								aPObj.start_date = firstDayOfNextMonth;
							}

							// Set proper product and newsub_variant ids
							const rotmRecordObj = rotmRecords.find(
								(f) => f.track_value === addonTrackNormalized
							);

							if (rotmRecordObj) {
								aPObj.meta_data.product.id =
									rotmRecordObj.product_id;
								// eslint-disable-next-line max-len
								aPObj.meta_data.product.variant_id =
									rotmRecordObj.newsub_variant_id;

								addonSubsPostObjs.push(aPObj);
							}
						});
					}
				}

				// Initialize queue
				const VMPQueue = new RedisUtil.VMPQueue();

				// Create Primary subscription's addon subscriptions -> if any
				if (type === 'primary' && addonSubsPostObjs.length > 0) {
					const addonPromises = addonSubsPostObjs.map(
						async (addonPObj) => {
							const addonVariantId = get(
								addonPObj,
								'meta_data.product.variant_id',
								false
							);
							const addonTrack = get(
								addonPObj,
								'meta_data.product.track',
								false
							);

							if (!addonVariantId || !addonTrack) {
								const err = new Error(
									'Missing track or variant_id on addon subscription.'
								);
								err.status = 500;
								throw err;
							}

							// Check inventory -> if newsub variant id is available
							const isAvailable =
								await shopifyService.checkIfVariantIsAvailable(
									addonVariantId
								);

							if (!isAvailable) {
								const err = new Error(
									`Sorry, ${addonTrack} record is no longer available.`
								);
								err.status = 422;
								throw err;
							}

							if (isOfferActive) {
								addonPObj.coupon_ids = [
									process.env.CHECKOUT_OFFER_REGULAR,
								];
							}

							// Create the addon subscription
							const { subscription: addonSubscription } =
								await chargebeeService.createCBCustomerSubscription(
									cbCustomerId,
									addonPObj,
									false
								);

							if (!addonSubscription || !addonSubscription.id) {
								const err = new Error(
									`Addon subscription was not created on create customer subscription (id: ${
										cbCustomerId || ''
									}).`
								);
								err.status = 500;
								throw err;
							}

							if (swapWindow === 'opened') {
								// Add to Queue -> Sync Swaps Analysis
								await VMPQueue.addToQueue(
									'sync_swap_analysis_job',
									{
										track: addonTrack,
										newAddon: true,
										subscription_id: addonSubscription.id,
									},
									5
								);
							}
						}
					);

					await Promise.all(addonPromises);
				}

				// Create CB subscription (primary or addon)
				const { subscription } =
					await chargebeeService.createCBCustomerSubscription(
						cbCustomerId,
						pObj,
						false
					);

				if (!subscription || !subscription.id) {
					const err = new Error(
						`Subscription was not created on create customer subscription (id: ${
							cbCustomerId || ''
						}).`
					);
					err.status = 500;
					throw err;
				}

				if (swapWindow === 'opened') {
					// Add to Queue -> Sync Swaps Analysis
					await VMPQueue.addToQueue(
						'sync_swap_analysis_job',
						{
							track,
							newPrimary: type === 'primary',
							newAddon: type === 'addon',
							subscription_id: subscription.id,
						},
						5
					);
				}

				// Adjust inventory
				if (
					(type === 'primary' && swapWindow === 'opened') ||
					type === 'addon'
				) {
					// UPDATE DB (Only when swap window is open)
					// newsub -1
					if (swapWindow === 'opened') {
						await connection.query(
							`UPDATE rotms_swap_products
                                SET available_newsub_quantity = available_newsub_quantity - 1,
                                    updated_at = current_timestamp
                                WHERE rotm = ? AND product_id = ? AND status = 1`,
							[currentRotmId, productId]
						);
					}

					// Add to Queue -> Adjust variant's inventory
					await VMPQueue.addToQueue('adjust_variant_inventory_job', {
						variantId,
						availableAdjustment: -1,
					});
				}

				// Add club-active-member tag
				if (type === 'primary') {
					await addClubActiveMemberTag(shCustomer, shopifyService);
				}

				// Commit
				await connection.commit();
			} catch (err) {
				await connection.rollback();
				throw err;
			} finally {
				connection.release();
			}

			return res.status(200).send({ success: true });
		} catch (error) {
			return next(error);
		}
	}
	// Update subscription Track
	async function updateSubscriptionTrack(req, res, next) {
		try {
			const {
				cb_customer_id: cbCustomerId,
				have_swapped: haveSwapped,
				previous_track: previousTrack,
				variant_id: variantId,
				track: postedTrack,
			} = req.body;

			// Validate
			if (!cbCustomerId || !variantId || !postedTrack) {
				const err = new Error(
					'Missing params on update subscription track.'
				);
				err.status = 422;
				throw err;
			}

			// Connect to DB
			const pool = DB.getInstance();
			const connection = await pool.getConnection();

			// Begin Transaction
			await connection.beginTransaction();

			try {
				// Validate track
				const track = await checkIfTrackIsValid({
					connection,
					track: postedTrack,
				});

				// GET CB subscription data from customer id
				const subscriptions =
					await chargebeeService.getCBCustomerSubscriptions(
						cbCustomerId
					);

				if (!subscriptions || subscriptions.length === 0) {
					const err = new Error(
						'No subscriptions were found on update subscription track.'
					);
					err.status = 404;
					throw err;
				}

				// Normalize
				const { primarySubscription } =
					normalizeSubscriptions(subscriptions);

				if (!primarySubscription) {
					const err = new Error(
						'No primary/gift subscription was found on update subscription track.'
					);
					err.status = 404;
					throw err;
				}

				const cbSubMetaData = primarySubscription.meta_data;

				if (!cbSubMetaData) {
					const err = new Error(
						`No meta data were found on update subscription track (subscription_id: ${primarySubscription.id}, customer_id: ${cbCustomerId}).`
					);
					err.status = 404;
					throw err;
				}

				const cbSubMetaDataTrack = get(
					cbSubMetaData,
					'product.track',
					false
				);

				if (!cbSubMetaDataTrack) {
					const err = new Error(
						`No meta data track was found on update subscription track (subscription_id: ${primarySubscription.id}, customer_id: ${cbCustomerId}).`
					);
					err.status = 404;
					throw err;
				}

				// Set track on meta_data
				cbSubMetaData.product.track = track;

				// SELECT current ROTM
				const { swapWindow, currentRotmId } = await fetchCurrentROTM({
					connection,
				});

				// If swap window is opened -> check for quantity and update inventory
				if (swapWindow === 'opened') {
					// Redis instance
					const VMPQueue = new RedisUtil.VMPQueue();

					// Check if available
					// Inventory stop flag can be set from env variables (INVENTORY_STOP_QUANTITY)
					const inventoryStopFlag =
						process.env.INVENTORY_STOP_QUANTITY || 0;
					const [rows] = await connection.query(
						`SELECT id
                            FROM rotms_swap_products
                            WHERE rotm = ? AND category = 'rotm' AND swap_variant_id = ?
                                AND available_swap_quantity > ? AND status = 1
                            LIMIT 0,1`,
						[currentRotmId, variantId, inventoryStopFlag]
					);

					// Check if available
					if (!rows || rows.length === 0) {
						const err = new Error(
							'Variant is not available on update subscription track'
						);
						err.status = 422;
						throw err;
					}

					// Update swap +1 on previous product
					// Only if swap window is open and user have not swapped
					if (!haveSwapped) {
						// GET previous track id
						const [tRows] = await connection.query(
							`SELECT id FROM tracks
                                WHERE value = ? AND status = 1
                                LIMIT 0,1`,
							[previousTrack]
						);

						// Previous track id
						const trackId = get(tRows, '[0].id', false);

						if (trackId) {
							// Find previous product swap variant id from DB
							const [sRows] = await connection.query(
								`SELECT swap_variant_id
                                    FROM rotms_swap_products
                                    WHERE rotm = ? AND category = 'rotm' AND track = ? AND status = 1
                                    LIMIT 0,1`,
								[currentRotmId, trackId]
							);

							const previousSwapVariantId = get(
								sRows,
								'[0].swap_variant_id',
								false
							);

							if (previousSwapVariantId) {
								// UPDATE DB
								// swap +1 on previous ROTM
								await connection.query(
									`UPDATE rotms_swap_products
                                        SET available_swap_quantity = available_swap_quantity + 1,
                                            updated_at = current_timestamp
                                        WHERE rotm = ? AND swap_variant_id = ? AND status = 1`,
									[currentRotmId, previousSwapVariantId]
								);

								// Add to Queue -> Adjust variant's inventory
								// swap +1
								await VMPQueue.addToQueue(
									'adjust_variant_inventory_job',
									{
										variantId: previousSwapVariantId,
										availableAdjustment: 1,
									}
								);
							}
						}
					}

					// UPDATE DB
					// swap -1 on selected
					await connection.query(
						`UPDATE rotms_swap_products
                            SET available_swap_quantity = available_swap_quantity - 1,
                                updated_at = current_timestamp
                            WHERE rotm = ? AND swap_variant_id = ? AND available_swap_quantity > 0 AND status = 1`,
						[currentRotmId, variantId]
					);

					// Add to Queue -> Adjust variant's inventory
					// swap -1
					await VMPQueue.addToQueue('adjust_variant_inventory_job', {
						variantId,
						availableAdjustment: -1,
					});
				}

				// Update CB subscription with new meta_data/track
				const { subscription: updatedCBSubscription } =
					await chargebeeService.updateCBCustomerSubscription(
						primarySubscription.id,
						{
							meta_data: cbSubMetaData,
						}
					);

				// Validate
				if (!updatedCBSubscription || !updatedCBSubscription.id) {
					const err = new Error(
						`Subscription was not updated on updateSubscriptionTrack (subscription_id: ${primarySubscription.id}, customer_id: ${cbCustomerId}).`
					);
					err.status = 500;
					throw err;
				}

				// Commit
				await connection.commit();
			} catch (err) {
				await connection.rollback();
				throw err;
			} finally {
				connection.release();
			}

			return res.status(200).send({ success: true });
		} catch (error) {
			return next(error);
		}
	}

	// Update Subscription Term
	async function updateSubscriptionTerm(req, res, next) {
		try {
			const { cb_customer_id: cbCustomerId, cb_plan_id: cbPlanId } =
				req.body;

			// Validate
			if (!cbCustomerId || !cbPlanId) {
				const err = new Error(
					'Missing params on update subscription term.'
				);
				err.status = 422;
				throw err;
			}

			// GET CB subscription data from customer id
			const { period: cbPlanPeriod } = await chargebeeService.getCBPlan(
				cbPlanId
			);

			if (!cbPlanPeriod) {
				const err = new Error(
					`Plan period wasn't found on update subscription term (${cbPlanId}).`
				);
				err.status = 404;
				throw err;
			}

			// GET CB subscription data from customer id
			const subscriptions =
				await chargebeeService.getCBCustomerSubscriptions(cbCustomerId);

			if (!subscriptions || subscriptions.length === 0) {
				const err = new Error(
					'No subscriptions were found on update subscription term.'
				);
				err.status = 404;
				throw err;
			}

			// Normalize
			const { primarySubscription } =
				normalizeSubscriptions(subscriptions);

			if (!primarySubscription) {
				const err = new Error(
					'No primary/gift subscription was found on update subscription term.'
				);
				err.status = 404;
				throw err;
			}

			// Subscription type (primary or gift)
			const primarySubType = get(
				primarySubscription,
				'meta_data.type',
				false
			);
			// Subscription plan id
			const primarySubPlanId = get(primarySubscription, 'plan_id', false);

			let couponID = [];

			// Get next billing date
			const nextBillingAt = get(
				primarySubscription,
				'next_billing_at',
				false
			);

			if (nextBillingAt) {
				const nextBillingDt = DateTime.fromSeconds(nextBillingAt);

				if (nextBillingDt.isValid) {
					const fromDt = DateTime.local()
						.setZone('America/Denver')
						.endOf('month')
						.minus({ days: 1 });
					const toDt = DateTime.local()
						.setZone('America/Denver')
						.startOf('month')
						.plus({ months: 1, days: 2 });

					if (
						Interval.fromDateTimes(fromDt, toDt).contains(
							nextBillingDt
						) &&
						cbPlanPeriod > 1 &&
						primarySubscription.status !== 'future' &&
						primarySubType === 'primary' &&
						primarySubPlanId &&
						!primarySubPlanId.endsWith('-new')
					) {
						couponID =
							cbPlanPeriod === 6
								? process.env.GIFT_BRIDGE_6_MONTHS
								: process.env.GIFT_BRIDGE_12_MONTHS;
					}
				}
			}

			// POST data
			const pObj = {
				plan_id: cbPlanId,
				end_of_term: primarySubscription.status !== 'future',
				coupon_ids: [couponID],
			};

			// Update CB subscription with new term
			const { subscription: updatedCBSubscription } =
				await chargebeeService.updateCBCustomerSubscription(
					primarySubscription.id,
					pObj
				);

			// Validate
			if (!updatedCBSubscription || !updatedCBSubscription.id) {
				const err = new Error(
					`Subscription was not updated on updateSubscriptionTerm (subscription_id: ${primarySubscription.id}, customer_id: ${cbCustomerId}).`
				);
				err.status = 500;
				throw err;
			}

			return res.status(200).send({ success: true });
		} catch (error) {
			return next(error);
		}
	}

	// Cancel subscription
	async function cancelSubscription(req, res, next) {
		try {
			const { id, type, end_of_term: endOfTerm } = req.body;

			// Validate id
			if (!id) {
				const err = new Error(
					'Missing params on remove customer subscription.'
				);
				err.status = 422;
				throw err;
			}

			// Validate type
			if (type !== 'addon') {
				const err = new Error(
					'Invalid subscription type on remove customer subscription.'
				);
				err.status = 422;
				throw err;
			}

			// Cancel on Chargebee
			const { subscription } =
				await chargebeeService.cancelCBSubscription(id, endOfTerm);

			const productId = get(subscription, 'meta_data.product.id', false);
			const isSwappedProduct = get(
				subscription,
				'meta_data.product.swapped',
				false
			);

			// Check if the product exists and the window is open and update inventory
			if (productId) {
				// Connect to DB
				const pool = DB.getInstance();
				const connection = await pool.getConnection();

				// Begin Transaction
				await connection.beginTransaction();

				try {
					// SELECT current ROTM
					const { swapWindow, currentRotmId } =
						await fetchCurrentROTM({ connection });

					// In swap window open...
					if (swapWindow === 'opened') {
						await updateCancelledSubscriptionInventory({
							connection,
							currentRotmId,
							productId,
							isSwappedProduct,
						});
					}

					// Commit
					await connection.commit();
				} catch (err) {
					await connection.rollback();
					throw err;
				} finally {
					connection.release();
				}
			}

			return res.status(200).send({ success: true });
		} catch (error) {
			return next(error);
		}
	}

	// Cancel all subscriptions
	async function cancelAllSubscriptions(req, res, next) {
		try {
			const {
				subscription_id: cbSubscriptionId,
				shopify_client_id: shCustomerId,
				chargebee_client_id: cbCustomerId,
				first_name: shCustomerFirstName,
				last_name: shCustomerLastName,
				email: shCustomerEmail,
				reason: cancelReason,
				comments: cancelComments,
				addons,
			} = req.body;

			// Validate id
			if (!cbSubscriptionId) {
				const err = new Error(
					'Missing params on cancel all customer subscriptions.'
				);
				err.status = 422;
				throw err;
			}

			// GET subscription status
			const cbSubscription = await chargebeeService.getCBSubscriptionById(
				cbSubscriptionId
			);

			// Validate subscription
			if (!cbSubscription) {
				const err = new Error(
					'Subscription not found on cancel all customer subscriptions.'
				);
				err.status = 422;
				throw err;
			}

			// Check status
			const status = get(cbSubscription, 'subscription.status', '');

			// Connect to DB
			const pool = DB.getInstance();
			const connection = await pool.getConnection();

			// Begin Transaction
			await connection.beginTransaction();

			try {
				// SELECT current ROTM
				const { swapWindow, currentRotmId } = await fetchCurrentROTM({
					connection,
				});

				// Cancel primary subscription on Chargebee
				const { subscription } =
					await chargebeeService.cancelCBSubscription(
						cbSubscriptionId,
						status !== 'future'
					);

				const productId = get(
					subscription,
					'meta_data.product.id',
					false
				);
				const isSwappedProduct = get(
					subscription,
					'meta_data.product.swapped',
					false
				);
				const productTrack = get(
					subscription,
					'meta_data.product.track',
					false
				);

				// Update primary subscription inventory
				if (productId && swapWindow === 'opened') {
					await updateCancelledSubscriptionInventory({
						connection,
						currentRotmId,
						productId,
						isSwappedProduct,
					});

					// Update swap analysis
					// Trigger Message via Socket
					await socketService.triggerMessage(
						'swaps-dashboard',
						'refresh-cancellations',
						{
							track: productTrack,
							type: 'primary',
						}
					);
				}

				// Loop addons (if any) and cancel
				if (addons && addons.length > 0) {
					const addonPromises = addons.map(async (addon) => {
						const { id, status: addonStatus } = addon;

						if (id) {
							const { subscription: addonSubscription } =
								await chargebeeService.cancelCBSubscription(
									id,
									addonStatus !== 'future'
								);

							const addonProductId = get(
								addonSubscription,
								'meta_data.product.id',
								false
							);
							const addonIsSwappedProduct = get(
								addonSubscription,
								'meta_data.product.swapped',
								false
							);
							const addonProductTrack = get(
								addonSubscription,
								'meta_data.product.track',
								false
							);

							// Update addon subscription inventory
							if (addonProductId && swapWindow === 'opened') {
								await updateCancelledSubscriptionInventory({
									connection,
									currentRotmId,
									productId: addonProductId,
									isSwappedProduct: addonIsSwappedProduct,
								});

								// Update swap analysis
								// Trigger Message via Socket
								await socketService.triggerMessage(
									'swaps-dashboard',
									'refresh-cancellations',
									{
										track: addonProductTrack,
										type: 'addon',
									}
								);
							}
						}
					});

					await Promise.all(addonPromises);
				} // End of Loop

				// Commit
				await connection.commit();
			} catch (err) {
				await connection.rollback();
				throw err;
			} finally {
				connection.release();
			}

			// Initialize queue
			const VMPQueue = new RedisUtil.VMPQueue();

			// Add to Queue -> Submit cancellation quiz
			await VMPQueue.addToQueue(
				'submit_cancellation_quiz_job',
				{
					subscription_id: cbSubscriptionId,
					shopify_client_id: shCustomerId,
					chargebee_client_id: cbCustomerId,
					first_name: shCustomerFirstName,
					last_name: shCustomerLastName,
					email: shCustomerEmail,
					reason: cancelReason,
					comments: cancelComments,
				},
				5
			);

			return res.status(200).send({ success: true });
		} catch (error) {
			return next(error);
		}
	}

	// reactivate subscription
	async function reactivateSubscription(req, res, next) {
		try {
			const { id, type, customer_id: cbCustomerId } = req.body;

			// Validate id
			if (!id || !type || !cbCustomerId) {
				const err = new Error(
					'Missing params on reactivate customer subscription.'
				);
				err.status = 422;
				throw err;
			}

			// Validate type
			if (!['primary', 'addon'].includes(type)) {
				const err = new Error(
					'Invalid subscription type on reactivate customer subscription.'
				);
				err.status = 422;
				throw err;
			}

			// Reactivate
			await chargebeeService.reactivateCBSubscription(
				id,
				type,
				cbCustomerId
			);

			// Deactivate cancellation quiz
			if (type === 'primary') {
				// Initialize queue
				const VMPQueue = new RedisUtil.VMPQueue();

				// Add to Queue -> Submit cancellation quiz
				await VMPQueue.addToQueue(
					'deactivate_cancellation_quiz_job',
					{ subscription_id: id, chargebee_client_id: cbCustomerId },
					5
				);
			}

			return res.status(200).send({ success: true });
		} catch (error) {
			return next(error);
		}
	}

	// POST
	async function swapSubscriptionRecord(req, res, next) {
		try {
			const {
				subscription_id: cbSubscriptionId,
				track,
				type,
				customer_id: customerId,
				gift_code: giftCode,
				gift_bundle: giftBundle,
				gifter_customer_id: gifterCustomerId,
				gifter_order_id: gifterOrderId,
				swapped_product_id: swappedProductId,
				product_id: productId,
				variant_id: variantId,
				title,
				vendor,
				image,
				handle,
			} = req.body;

			// Validate params
			if (
				!cbSubscriptionId ||
				!productId ||
				!variantId ||
				!track ||
				!type
			) {
				const err = new Error(
					'Missing params on swap subscription record.'
				);
				err.status = 422;
				throw err;
			}

			let success = false;
			const isSwapForCredit =
				parseInt(productId, 10) ===
				parseInt(process.env.SWAP_FOR_CREDIT_PRODUCT_ID, 10);

			// Connect to DB
			const pool = DB.getInstance();
			const connection = await pool.getConnection();

			// Begin Transaction
			await connection.beginTransaction();

			try {
				// Adjust inventory in our DB -> Exclude Swap for credit
				if (!isSwapForCredit) {
					// SELECT Current ROTM from rotm swap products
					// Only the available ( swap > 0 )
					// Inventory stop flag can be set from env variables (INVENTORY_STOP_QUANTITY)
					const inventoryStopFlag =
						process.env.INVENTORY_STOP_QUANTITY || 0;
					const [rows] = await connection.query(
						`SELECT r.id as rotm_id
                            FROM rotms r
                                INNER JOIN rotms_swap_products rp
                                ON rp.rotm = r.id AND rp.product_id = ?
                                    AND rp.available_swap_quantity > ? AND rp.status = 1
                            WHERE r.status = 2 LIMIT 0,1`,
						[productId, inventoryStopFlag]
					);

					// Check if available
					if (!rows || rows.length === 0) {
						const err = new Error(
							'Variant is not available on swap subscription record'
						);
						err.status = 422;
						throw err;
					}

					// ROTM id
					const rotmId = get(rows, '[0].rotm_id', false);

					// UPDATE Current ROTM record inventory
					// existingsub - 1
					// swap + 1
					const [{ affectedRows }] = await connection.query(
						`UPDATE rotms_swap_products
                            SET available_existingsub_quantity = available_existingsub_quantity - 1,
                                available_swap_quantity = available_swap_quantity + 1,
                                updated_at = current_timestamp
                            WHERE rotm = ? AND product_id = ? AND status = 1`,
						[rotmId, swappedProductId]
					);

					if (affectedRows !== 1) {
						const err = new Error(
							`Inventory was not updated (current_product_id: ${swappedProductId}).`
						);
						err.status = 500;
						throw err;
					}

					// UPDATE Selected Record inventory
					// swap - 1
					await connection.query(
						`UPDATE rotms_swap_products
                            SET available_swap_quantity = available_swap_quantity - 1,
                                updated_at = current_timestamp
                            WHERE rotm = ? AND product_id = ? AND available_swap_quantity > 0 AND status = 1`,
						[rotmId, productId]
					);

					// Commit
					await connection.commit();
				}

				// Subscription meta_data
				const pObj = {
					meta_data: {
						new: false,
						type,
						product: {
							id: productId,
							variant_id: variantId,
							track: track.toLowerCase(),
							swapped: true,
							swapped_date: Math.floor(
								DateTime.local().toSeconds()
							),
						},
					},
				};

				// Add gift meta_data -> if any
				if (customerId) {
					pObj.meta_data.customer_id = customerId;
				}

				if (giftCode) {
					pObj.meta_data.gift_code = giftCode;
				}

				if (giftBundle) {
					pObj.meta_data.gift_bundle = giftBundle;
				}

				if (gifterCustomerId) {
					pObj.meta_data.gifter_customer_id = gifterCustomerId;
				}

				if (gifterOrderId) {
					pObj.meta_data.gifter_order_id = gifterOrderId;
				}

				// Update CB subscription with the new meta_data
				const {
					subscription: updatedCBSubscription,
					customer: cbCustomer,
				} = await chargebeeService.updateCBCustomerSubscription(
					cbSubscriptionId,
					pObj
				);

				// Validate
				if (!updatedCBSubscription || !updatedCBSubscription.id) {
					const err = new Error(
						`Subscription was not updated (swapSubscriptionRecord) (subscription_id: ${cbSubscriptionId}).`
					);
					err.status = 500;
					throw err;
				}

				// Initialize queue
				const VMPQueue = new RedisUtil.VMPQueue();

				// Add to Queue -> Sync Swaps Analysis
				await VMPQueue.addToQueue(
					'sync_swap_analysis_job',
					{
						track,
						subscription_id: updatedCBSubscription.id,
						swappedProductId,
						swapProductId: productId,
						isSwapForCredit,
					},
					5
				);

				// Add to Queue -> Adjust Shopify inventory after swap
				// Triggers multiple jobs
				await VMPQueue.addToQueue(
					'adjust_inventory_after_swap_job',
					{
						swappedProductId,
						swapProductId: productId,
						swapVariantId: variantId,
					},
					5
				);

				// Send swap confirmation email
				const firstName = get(cbCustomer, 'first_name', '');
				const lastName = get(cbCustomer, 'last_name', '');
				const email = get(cbCustomer, 'email', null);

				if (email) {
					// Add to queue
					await VMPQueue.addToQueue(
						'send_swap_confirmation_email_job',
						{
							first_name: firstName,
							last_name: lastName,
							email,
							product_id: productId,
							variant_id: variantId,
							track,
							title,
							vendor,
							image,
							handle,
							is_swap_for_credit: isSwapForCredit,
						},
						5
					);
				}
			} catch (err) {
				await connection.rollback();
				throw err;
			} finally {
				connection.release();
			}

			success = true;
			return res
				.status(200)
				.send({ success, is_swap_for_credit: isSwapForCredit });
		} catch (error) {
			return next(error);
		}
	}

	// GET Renewal Estimate
	async function getRenewalEstimate(req, res, next) {
		try {
			const { subscriptionId: cbSubscriptionId } = req.query;

			// Validate id
			if (!cbSubscriptionId) {
				const err = new Error('Subscription not found.');
				err.status = 404;
				throw err;
			}

			const estimate =
				await chargebeeService.getCBSubscriptionRenewalEstimate(
					cbSubscriptionId
				);

			const discounts = get(
				estimate,
				'invoice_estimate.discounts',
				false
			);
			let subtotalAfterDiscounts = get(
				estimate,
				'invoice_estimate.sub_total',
				false
			);
			if (discounts) {
				discounts.forEach((discount) => {
					if (discount.description.includes('PROMO')) {
						subtotalAfterDiscounts -= discount.amount;
						discount.description = 'Renewal Offer';
					}
					if (discount.description.includes('Promotional Credits')) {
						subtotalAfterDiscounts -= discount.amount;
						discount.description = 'Membership Credits';
					}
				});
			}

			const estimateObj = {
				subscriptionId: get(
					estimate,
					'subscription_estimate.id',
					false
				),
				subtotal: get(estimate, 'invoice_estimate.sub_total', false),
				subtotalAfterDiscounts,
				discounts: get(estimate, 'invoice_estimate.discounts', false),
				total: get(estimate, 'invoice_estimate.total', false),
				taxes: get(estimate, 'invoice_estimate.taxes', false),
			};

			return res.status(200).send({ success: true, estimateObj });
		} catch (error) {
			return next(error);
		}
	}

	// Download Invoice
	async function downloadInvoice(req, res, next) {
		try {
			const { subscriptionId: cbSubscriptionId } = req.query;

			// Validate id
			if (!cbSubscriptionId) {
				const err = new Error('Subscription not found.');
				err.status = 404;
				throw err;
			}

			let invoiceURL = '';

			const invoice = await chargebeeService.getCBLatestInvoice(
				cbSubscriptionId
			);

			// Check if invoice ID exists and set the invoice URL
			const invoiceID = get(invoice, 'list[0].invoice.id', false);

			if (invoiceID) {
				invoiceURL = await chargebeeService.getCBInvoiceDownloadURL(
					invoiceID
				);
			}

			return res.status(200).send({ success: true, invoiceURL });
		} catch (error) {
			return next(error);
		}
	}

	// Swaps Feedback
	async function submitSwapsFeedback(req, res, next) {
		try {
			const { email, rating, text } = req.body;

			// Validate
			if (!email || !rating) {
				const err = new Error(
					'Missing params on swap feedback submission.'
				);
				err.status = 422;
				throw err;
			}

			// Connect to DB
			const pool = DB.getInstance();
			const connection = await pool.getConnection();

			await connection.beginTransaction();

			let success = false;

			try {
				const ratingValue = rating === 'positive' ? 1 : 0;
				const dateNow = DateTime.local().plus({ month: 1 });
				const month = dateNow.toFormat('MM');
				const year = dateNow.toFormat('yyyy');
				const month_name = dateNow.toFormat('MMMM');

				// SELECT -> check if entry already added
				const [rows] = await connection.query(
					'SELECT id FROM swaps_feedback WHERE email = ? AND month = ? AND year = ? LIMIT 0,1',
					[email, month, year]
				);

				// Early return -> record already added
				if (rows && rows.length > 0) {
					//     const id = get(rows, '[0].id', false);

					//     // UPDATE updated_at date
					//     const [{ affectedRows }] = await connection.query(
					//         'UPDATE swaps_feedback SET ? WHERE id = ? LIMIT 1',
					//         [{ updated_at: getCurrentTS(), status: 1 }, id]
					//     );

					//     if (affectedRows !== 1) {
					//         const err = new Error(`Waitlist record was not updated for ${email} (product_id: ${productId}).`);
					//         err.status = 500;
					//         throw err;
					//     }

					// Commit
					await connection.commit();

					return res
						.status(200)
						.send({ success: true, already_added: true });
				}

				// Check if Shopify customer exists
				let shCustomerId = null;
				let firstName = null;
				let lastName = null;
				const shCustomer = await shopifyService.fetchShopifyCustomer({
					filterBy: 'email',
					value: email,
				});

				if (shCustomer && shCustomer.id) {
					shCustomerId = shCustomer.id;
					firstName = shCustomer.first_name || null;
					lastName = shCustomer.last_name || null;
				}

				// Check if Chargebee customer exists
				let cbCustomerId = null;
				const { customer: cbCustomer } =
					await chargebeeService.getCBCustomerByEmail(email);

				if (cbCustomer && cbCustomer.id) {
					cbCustomerId = cbCustomer.id;
				}

				// Fields to insert
				const fields = {
					email: email.toLowerCase().trim(),
					first_name: firstName,
					last_name: lastName,
					sh_customer_id: shCustomerId,
					cb_customer_id: cbCustomerId,
					rating: ratingValue,
					text,
					month,
					month_name,
					year,
					created_at: getCurrentTS(),
					status: 1,
				};

				// INSERT new record
				const [{ insertId }] = await connection.query(
					'INSERT INTO swaps_feedback SET ? ',
					[fields]
				);

				if (!insertId) {
					const err = new Error(
						`Swaps Feedback record was not created for ${email}).`
					);
					err.status = 500;
					throw err;
				}

				// Trigger Message via Socket
				// await socketService.triggerMessage('swaps-feedback', 'refresh');

				// Create metafield
				const { id: metafield_id } =
					await shopifyService.createMetafield({
						namespace: 'swaps',
						key: 'feedback',
						type: 'single_line_text_field',
						// value_type: 'string',
						value: `${month}_${year}`,
						owner_resource: 'customer',
						owner_id: shCustomerId,
					});

				if (!metafield_id) {
					const err = new Error(
						`Metafield could not be created for ${email}).`
					);
					err.status = 500;
					throw err;
				}

				// Commit if no errors
				await connection.commit();

				const VMPQueue = new RedisUtil.VMPQueue();
				await VMPQueue.addToQueue('swaps-feedback-job', {
					swapFeedback: { ...fields },
				});

				success = true;
			} catch (err) {
				await connection.rollback();
				throw err;
			} finally {
				connection.release();
			}

			return res.status(200).send({ success });
		} catch (error) {
			return next(error);
		}
	}

	return {
		getSubscriptions,
		createSubscription,
		updateSubscriptionTrack,
		updateSubscriptionTerm,
		cancelSubscription,
		cancelAllSubscriptions,
		reactivateSubscription,
		swapSubscriptionRecord,
		submitSwapsFeedback,
		getRenewalEstimate,
		downloadInvoice,
	};
}

module.exports = subscriptionsController;
