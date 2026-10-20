// backend/api/src/routes/jobRoutes.js (Excerpt)

import { Router } from 'express';
import { authenticate } from '../middleware/auth.js';
import { cancelJob } from '../services/jobService.js';
import { logger } from '../utils/logger.js';

const router = Router();

/**
 * POST /api/jobs/:jobId/cancel
 * Cancels a queued or running job if owned by the authenticated user.
 */
router.post('/:jobId/cancel', authenticate, async (req, res) => {
  try {
    const { jobId } = req.params;
    const userId = req.user.id;

    const result = await cancelJob(jobId, userId);

    if (!result.success) {
      const statusCode = result.error.includes('Unauthorized') ? 403 : 400;
      return res.status(statusCode).json({ success: false, error: result.error });
    }

    return res.status(200).json({
      success: true,
      message: `Job ${jobId} successfully cancelled.`,
      data: result.job,
    });
  } catch (err) {
    logger.error({ err, jobId: req.params.jobId }, '[jobRoutes] Error cancelling job');
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

export default router;
