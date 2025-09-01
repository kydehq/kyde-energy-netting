
"""add signature to policies

Revision ID: 001_add_signature
Revises: 
Create Date: 2025-09-01

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '001_add_signature'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Add signature column to policies table
    op.add_column('policies', sa.Column('signature', sa.String(length=512), nullable=True))

def downgrade():
    # Remove signature column from policies table
    op.drop_column('policies', 'signature')