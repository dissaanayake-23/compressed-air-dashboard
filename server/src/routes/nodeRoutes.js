import { Router } from 'express';
import { getNodes, getNodeById, createNode, updateNode, deleteNode } from '../controllers/nodeController.js';

const router = Router();

router.get('/', getNodes);
router.get('/:id', getNodeById);
router.post('/', createNode);
router.put('/:id', updateNode);
router.delete('/:id', deleteNode);

export default router;
