select ec.intCandidateId, ae.intAttachmentId, em.msgfilename as attachmentName, vchAttachmentName, vchFileType--, a.vchAttachmentName
from lEventCandidate ec left join dEvent e on ec.intEventId = e.intEventId
				--left join dCandidate c on ec.intCandidateId = c.intCandidateId
				left join lAttachmentEvent ae on ec.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
where (ae.intAttachmentId is not null and vchFileType <> '.eml') or em.AttachmentID is not null
--where em.AttachmentID is not null