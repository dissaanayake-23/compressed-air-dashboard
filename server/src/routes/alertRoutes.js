import { Router } from 'express';
import { getAlerts, resolveAlert, createAlert, deleteAlert } from '../controllers/alertController.js';

const router = Router();

router.get('/', getAlerts);
router.post('/', createAlert);
router.patch('/:id/resolve', resolveAlert);
router.delete('/:id', deleteAlert);

export default router;
